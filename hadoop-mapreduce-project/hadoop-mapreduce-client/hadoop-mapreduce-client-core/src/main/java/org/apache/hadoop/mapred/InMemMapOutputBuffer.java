package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.MapTask;
//import org.apache.hadoop.mapred.MapTask.APPROX_HEADER_LENGTH;
import org.apache.hadoop.mapred.MapTask.MapBufferTooSmallException;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.buffer.impl.Buffer;
import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;




//@InterfaceAudience.LimitedPrivate({"MapReduce"})
//@InterfaceStability.Unstable
//public static class InMemMapOutputBuffer<K extends Object, V extends Object>
public class InMemMapOutputBuffer<K extends Object, V extends Object> extends Buffer<K, V>
    implements MapOutputCollector<K, V>, IndexedSortable {

	private class PartitionBufferMerger {

		private int snapshots;

		public PartitionBufferMerger() throws IOException {
			this.snapshots = 0;
		}

		/**
		 * Create the final output file from all spill files.
		 * Spill files are not deleted.
		 * @throws IOException
		 */
		public synchronized OutputFile mergeFinal() throws IOException {
			List<PartitionBufferFile> finalSpills = new ArrayList<PartitionBufferFile>();
			long finalDataSize = 0;
			long indexFileSize = partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

			for (PartitionBufferFile spill : spills) {
				if (spill.valid()) {
					finalSpills.add(spill);
					finalDataSize += spill.dataSize();
				}
			}
			LOG.info("PartitionBufferMerger: final merge size " + finalDataSize + ". Spill files: " + finalSpills.toString());

			Path dataFile = outputHandle.getOutputFileForWrite(taskid, finalDataSize);
			Path indexFile = outputHandle.getOutputIndexFileForWrite(taskid, indexFileSize);
			PartitionBufferFile finalOutput = new PartitionBufferFile(0, dataFile, indexFile, 1f, true);
			merge(finalSpills, finalOutput);
			return new OutputFile(taskid, -1, 1f, finalOutput.data, finalOutput.index, true, partitions);
		}

		/**
		 * Used by the reducer to keep input data files
		 * from growing too large.
		 * @throws IOException
		 */
		public synchronized SortedSet<OutputFile> mergeSpill(int start, int end) throws IOException {
			List<PartitionBufferFile> mergeSpills = new ArrayList<PartitionBufferFile>();
			long dataFileSize = 0;
			long indexFileSize = partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

			boolean eof = false;
			float progress = 0f;
			SortedSet<Integer> spillids = new TreeSet<Integer>();
			for (int i = start; i <= end; i++) {
				if (spills.get(i).valid()) {
					mergeSpills.add(spills.get(i));
					dataFileSize += spills.get(i).dataSize();
					eof = eof || spills.get(i).eof;
					progress = spills.get(i).progress;
					spillids.add(i);
				}
			}
			LOG.info("PartitionBufferMerger: intermediate merge. Total size " + 
					dataFileSize + ". Total spill files: " + mergeSpills.toString());


			int snapshotId = snapshots++;
			Path dataFile = outputHandle.getOutputSnapshotFileForWrite(taskid, snapshotId, dataFileSize);
			Path indexFile = outputHandle.getOutputSnapshotIndexFileForWrite(taskid, snapshotId, indexFileSize);
			PartitionBufferFile snapshot = new PartitionBufferFile(-1, dataFile, indexFile, progress, eof);

			merge(mergeSpills, snapshot);
			
			SortedSet<OutputFile> outputs = new TreeSet<OutputFile>();
			outputs.add(new OutputFile(taskid, spillids, progress, snapshot.data, snapshot.index, eof, partitions));
			for (PartitionBufferFile spill : mergeSpills) {
				OutputFile file = new OutputFile(taskid, spill.id, spill.progress,
						                        spill.data, spill.index, spill.eof, partitions);
				outputs.add(file);
			}
			
			return outputs;
		}
		
		/**
		 * Generate snapshot output file.
		 * @return The snapshot output file.
		 * @throws IOException
		 */
/*
		public synchronized OutputFile mergeSnapshot() throws IOException {
			List<PartitionBufferFile> mergeSpills = new ArrayList<PartitionBufferFile>();
			long dataSize = 0;
			long indexSize = partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

			float progress = 0f;
			boolean eof = false;
			for (PartitionBufferFile spill : spills) {
				if (spill.valid()) {
					LOG.debug("Merge spill " + spill);
					mergeSpills.add(spill);
					dataSize += spill.dataSize();
					eof = eof || spill.eof;
					progress = spill.progress;
				}
			}

			int snapshotId = snapshots++;
			Path dataFile = outputHandle.getOutputSnapshotFileForWrite(taskid, snapshotId, dataSize);
			Path indexFile = outputHandle.getOutputSnapshotIndexFileForWrite(taskid, snapshotId, indexSize);
			PartitionBufferFile snapshot = new PartitionBufferFile(-1, dataFile, indexFile, progress, eof);

			merge(mergeSpills, snapshot);
			//reset(true);
			return new OutputFile(taskid, progress, snapshot.data, snapshot.index, partitions);
		}
*/
		
		public synchronized OutputFile mergeStream(long sequence) throws IOException {
			List<PartitionBufferFile> mergeSpills = new ArrayList<PartitionBufferFile>();
			long dataSize = 0;
			long indexSize = partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

			float progress = 0f;
			boolean eof = false;
			for (PartitionBufferFile spill : spills) {
				if (spill.valid()) {
					LOG.debug("Merge spill " + spill);
					mergeSpills.add(spill);
					dataSize += spill.dataSize();
					eof = eof || spill.eof;
					progress = spill.progress;
				}
			}
			
			if (mergeSpills.size() == 0) {
				LOG.info("Merge stream: no spill files.");
				return null;
			}

			int snapshotId = snapshots++;
			Path dataFile = outputHandle.getOutputSnapshotFileForWrite(taskid, snapshotId, dataSize);
			Path indexFile = outputHandle.getOutputSnapshotIndexFileForWrite(taskid, snapshotId, indexSize);
			PartitionBufferFile snapshot = new PartitionBufferFile(-1, dataFile, indexFile, progress, eof);

			merge(mergeSpills, snapshot);
			LOG.info("Stream snapshot size " + snapshot.dataSize());
			return new OutputFile(taskid, sequence, snapshot.data, snapshot.index, partitions);
		}

		private void merge(List<PartitionBufferFile> spills, PartitionBufferFile output) throws IOException {
			if (spills.size() == 1) {
				output.copy(spills.get(0));
				return;
			}

			FSDataOutputStream dataOut = rfs.create(output.data, true);
			FSDataOutputStream indexOut = rfs.create(output.index, true);

			if (spills.size() == 0) {
				//create dummy files
				writeEmptyOutput(dataOut, indexOut);
				dataOut.close();
				indexOut.close();
			} else {
				for (int parts = 0; parts < partitions; parts++){
					//create the segments to be merged
					List<Segment<K, V>> segmentList =
						new ArrayList<Segment<K, V>>(spills.size());
					for(PartitionBufferFile spill : spills) {
						FSDataInputStream indexIn = rfs.open(spill.index);
						indexIn.seek(parts * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
						long segmentOffset = indexIn.readLong();
						long rawSegmentLength = indexIn.readLong();
						long segmentLength = indexIn.readLong();
						indexIn.close();
						FSDataInputStream in = rfs.open(spill.data);
						in.seek(segmentOffset);
						Segment<K, V> s =
							new Segment<K, V>(new IFile.Reader<K, V>(job, in, 
									          segmentLength, codec, null), true);
						segmentList.add(s);
					}

					//merge
					@SuppressWarnings("unchecked")
					RawKeyValueIterator kvIter =
						Merger.merge(job, rfs,
								keyClass, valClass,
								segmentList, job.getInt("io.sort.factor", 100),
								new Path(taskid.toString()),
								job.getOutputKeyComparator(), reporter, null, null, null);

					//write merged output to disk
					long segmentStart = dataOut.getPos();
					IFile.Writer<K, V> writer =
						new IFile.Writer<K, V>(job, dataOut, keyClass, valClass, codec, null);
					if (null == combinerClass || spills.size() < minSpillsForCombine) {
						Merger.writeFile(kvIter, writer, reporter, job);
					} else {
            CombineOutputCollector combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, job);
						//CombineOutputCollector combineCollector = new CombineOutputCollector();
						combineCollector.setWriter(writer);
						combineAndSpill(combineCollector, kvIter);
					}

					//close
					writer.close();

					//write index record
					writeIndexRecord(indexOut, dataOut, segmentStart, writer);
				}
				dataOut.close();
				indexOut.close();
			}
		}
	}

	private class PartitionBufferFile {
		int id;
		
		Path data;

		Path index;

		boolean valid;
		
		float progress;
		
		boolean eof;
		
		public PartitionBufferFile(int id, Path data, Path index, float progress, boolean eof) {
			this.id = id;
			this.data = data;
			this.index = index;
			this.valid = true;
			this.progress = progress;
			this.eof = eof;
		}

		@Override
		public String toString() {
			return data.getName() + "[" + dataSize() + " bytes]";
		}

		public void copy(PartitionBufferFile file) throws IOException {
			if (!file.valid()) {
				throw new IOException("PartitionBufferFile: copy from an unvalid file!");
			}
			this.id = file.id;
			this.progress = file.progress;
			this.eof = file.eof;
			this.valid = true;
			
			rfs.copyFromLocalFile(file.data, this.data);
			rfs.copyFromLocalFile(file.index, this.index);
		}

		public long dataSize() {
			try {
				return valid ? rfs.getFileStatus(data).getLen() : 0;
			} catch (IOException e) {
				e.printStackTrace();
				return 0;
			}
		}

		public boolean valid() {
			return this.valid;
		}

		public void delete() throws IOException {
			this.valid = false;
			if (rfs.exists(data)) {
				rfs.delete(data, true);
			}
			if (rfs.exists(index)) {
				rfs.delete(index, true);
			}
		}
	}


  private static final Log LOG = LogFactory.getLog(InMemMapOutputBuffer.class.getName());

  private int partitions;
  private JobConf job; 
  private TaskAttemptID taskid;
  private TaskReporter reporter;
  private Class<K> keyClass;
  private Class<V> valClass;
  private RawComparator<K> comparator;
  private SerializationFactory serializationFactory;
  private Serializer<K> keySerializer;
  private Serializer<V> valSerializer;
  private CombinerRunner<K,V> combinerRunner;
  private CombineOutputCollector<K, V> combineCollector;

  // Compression for map-outputs
  private CompressionCodec codec;

  // k/v accounting
  private IntBuffer kvmeta; // metadata overlay on backing store
  int kvstart;            // marks origin of spill metadata
  int kvend;              // marks end of spill metadata
  int kvindex;            // marks end of fully serialized records

  int equator;            // marks origin of meta/serialization
  int bufstart;           // marks beginning of spill
  int bufend;             // marks beginning of collectable
  int bufmark;            // marks end of record
  int bufindex;           // marks end of collected
  int bufvoid;            // marks the point where we should stop
                          // reading at the end of the buffer

  byte[] kvbuffer;        // main output buffer
  private final byte[] b0 = new byte[0];

  private static final int VALSTART = 0;         // val offset in acct
  private static final int KEYSTART = 1;         // key offset in acct
  private static final int PARTITION = 2;        // partition offset in acct
  private static final int VALLEN = 3;           // length of value
  private static final int NMETA = 4;            // num meta ints
  private static final int METASIZE = NMETA * 4; // size in bytes

  // spill accounting
  private List<PartitionBufferFile> spills = new ArrayList<PartitionBufferFile>();
  private int maxRec;
  private int softLimit;
  boolean spillInProgress;;
  int bufferRemaining;
  volatile Throwable sortSpillException = null;

  int numSpills = 0;
  private int minSpillsForCombine;
  private IndexedSorter sorter;
  final ReentrantLock spillLock = new ReentrantLock();
  final Condition spillDone = spillLock.newCondition();
  final Condition spillReady = spillLock.newCondition();
  final BlockingBuffer bb = new BlockingBuffer();
  volatile boolean spillThreadRunning = false;
  SpillThread spillThread = new SpillThread();
  private PartitionBufferMerger merger;

  private boolean pipeline = false; // for now, we're not supporting pipelineing, so just leave this as false

  private boolean eof = false;

  private FileSystem rfs;
  private FileHandle outputHandle = null;

  // Counters
  private Counters.Counter mapOutputByteCounter;
  private Counters.Counter mapOutputRecordCounter;
  private Counters.Counter fileOutputByteCounter;
  //private Counters.Counter reduceCombineInputCounter =
  //  getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
  private Counters.Counter combineOutputCounter; 

  final ArrayList<SpillRecord> indexCacheList =
    new ArrayList<SpillRecord>();
  private int totalIndexCacheMemory;
  private int indexCacheMemoryLimit;
  private static final int INDEX_CACHE_MEMORY_LIMIT_DEFAULT = 1024 * 1024;

  private MapTask mapTask;
  private MapOutputFile mapOutputFile;
  private Progress sortPhase;
  private Counters.Counter spilledRecordsCounter;

  public InMemMapOutputBuffer() {
  }

  @SuppressWarnings("unchecked")
  public void init(MapOutputCollector.Context context
                  ) throws IOException, ClassNotFoundException {
    job = context.getJobConf();
    reporter = context.getReporter();
    mapTask = context.getMapTask();
    taskid = mapTask.getTaskID();
    mapOutputFile = mapTask.getMapOutputFile();
    sortPhase = mapTask.getSortPhase();
    spilledRecordsCounter = reporter.getCounter(TaskCounter.SPILLED_RECORDS);
    rfs = ((LocalFileSystem)FileSystem.getLocal(job)).getRaw();
    this.combineOutputCounter = reporter.getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);

    this.spillThread = new SpillThread();
    this.spillThread.setDaemon(true);
    this.spillThread.start();

    this.merger = new PartitionBufferMerger();

    partitions = job.getNumReduceTasks();

    //sanity checks
    final float spillper =
      job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
    final int sortmb = job.getInt(JobContext.IO_SORT_MB, 100);
    indexCacheMemoryLimit = job.getInt(JobContext.INDEX_CACHE_MEMORY_LIMIT,
                                       INDEX_CACHE_MEMORY_LIMIT_DEFAULT);
    if (spillper > (float)1.0 || spillper <= (float)0.0) {
      throw new IOException("Invalid \"" + JobContext.MAP_SORT_SPILL_PERCENT +
          "\": " + spillper);
    }
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException(
          "Invalid \"" + JobContext.IO_SORT_MB + "\": " + sortmb);
    }
    sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
          QuickSort.class, IndexedSorter.class), job);
    // buffers and accounting
    int maxMemUsage = sortmb << 20;
    maxMemUsage -= maxMemUsage % METASIZE;
    kvbuffer = new byte[maxMemUsage];
    bufvoid = kvbuffer.length;
    kvmeta = ByteBuffer.wrap(kvbuffer)
       .order(ByteOrder.nativeOrder())
       .asIntBuffer();
    setEquator(0);
    bufstart = bufend = bufindex = equator;
    kvstart = kvend = kvindex;

    maxRec = kvmeta.capacity() / NMETA;
    softLimit = (int)(kvbuffer.length * spillper);
    bufferRemaining = softLimit;
    if (LOG.isInfoEnabled()) {
      LOG.info(JobContext.IO_SORT_MB + ": " + sortmb);
      LOG.info("soft limit at " + softLimit);
      LOG.info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "; length = " + maxRec);
    }

    // k/v serialization
    comparator = job.getOutputKeyComparator();
    keyClass = (Class<K>)job.getMapOutputKeyClass();
    valClass = (Class<V>)job.getMapOutputValueClass();
    serializationFactory = new SerializationFactory(job);
    keySerializer = serializationFactory.getSerializer(keyClass);
    keySerializer.open(bb);
    valSerializer = serializationFactory.getSerializer(valClass);
    valSerializer.open(bb);

    // output counters
    mapOutputByteCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
    mapOutputRecordCounter =
      reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    fileOutputByteCounter = reporter
        .getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);

    // compression
    if (job.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        job.getMapOutputCompressorClass(DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    } else {
      codec = null;
    }

    // combiner
    final Counters.Counter combineInputCounter =
      reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    combinerRunner = CombinerRunner.create(job, getTaskID(), 
                                           combineInputCounter,
                                           reporter, null);
    if (combinerRunner != null) {
      combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, job);
    } else {
      combineCollector = null;
    }
    spillInProgress = false;
    minSpillsForCombine = job.getInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3);
    spillThread.setDaemon(true);
    spillThread.setName("SpillThread");
    spillLock.lock();
    try {
      spillThread.start();
      while (!spillThreadRunning) {
        spillDone.await();
      }
    } catch (InterruptedException e) {
      throw new IOException("Spill thread failed to initialize", e);
    } finally {
      spillLock.unlock();
    }
    if (sortSpillException != null) {
      throw new IOException("Spill thread failed to initialize",
          sortSpillException);
    }
  }

  public void close() {
  }

  /*
  public synchronized void force() throws IOException {
    sortAndSpill();
  }

  private void reset(boolean restart) {
    try {
      if (spillThread != null) {
        LOG.debug("Close spill thread.");
        //spillThread.close();
        spillThread = null;
        LOG.debug("Spill thread closed.");
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    eof = false;
    bufindex = 0;
    bufvoid  = kvbuffer.length;
    kvstart = kvend = kvindex = 0;
    bufstart = bufend = bufindex = bufmark = 0;

    if (restart) {
      LOG.debug("Clear spill files.");
      // reset buffer variables. 
      for (PartitionBufferFile spill : spills) {
        try {
          spill.delete();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      spills.clear();

      LOG.debug("Start new spill thread.");
      // restart threads. 
      this.spillThread = new SpillThread();
      this.spillThread.setDaemon(true);
      this.spillThread.start();
    }
  }
  */

  /*
  public synchronized OutputFile close() throws IOException {
    LOG.debug("PartitionBuffer: closed called at progress " + progress.get());
    this.eof = true;
    OutputFile finalOutput = flush();
    return finalOutput;
  }
  */

  /*
  public synchronized OutputFile snapshot() throws IOException {
    LOG.debug("JBuffer " + taskid + " performing snapshot. progress " + progress.get());
    //spillThread.forceSpill();
    sortAndSpill();
    OutputFile snapshot = merger.mergeSnapshot();
    return snapshot;
  }

  public synchronized void stream(long sequence, boolean reset) throws IOException {
    LOG.debug("JBuffer " + taskid + " performing stream snapshot. sequence " + sequence);
    //spillThread.forceSpill();
    sortAndSpill();
    OutputFile stream = merger.mergeStream(sequence);
    if (stream != null ) {
      umbilical.output(stream);
      if (reset) reset(true);
    }
  }

  public synchronized void malloc() {
    kvbuffer = new byte[(int)kvbufferSize];
    reset(true);
  }

  public synchronized void free() {
    reset(false);
    kvbuffer = null;
  }
  */

  /**
   * Serialize the key, value to intermediate storage.
   * When this method returns, kvindex must refer to sufficient unused
   * storage to store one METADATA.
   */
  public synchronized void collect(K key, V value, final int partition
                                   ) throws IOException {
    reporter.progress();
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
                            + keyClass.getName() + ", received "
                            + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
                            + valClass.getName() + ", received "
                            + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")");
    }
    checkSpillException();
    bufferRemaining -= METASIZE;
    if (bufferRemaining <= 0) {
      // start spill if the thread is not running and the soft limit has been
      // reached
      spillLock.lock();
      try {
        do {
          if (!spillInProgress) {
            final int kvbidx = 4 * kvindex;
            final int kvbend = 4 * kvend;
            // serialized, unspilled bytes always lie between kvindex and
            // bufindex, crossing the equator. Note that any void space
            // created by a reset must be included in "used" bytes
            final int bUsed = distanceTo(kvbidx, bufindex);
            final boolean bufsoftlimit = bUsed >= softLimit;
            if ((kvbend + METASIZE) % kvbuffer.length !=
                equator - (equator % METASIZE)) {
              // spill finished, reclaim space
              resetSpill();
              bufferRemaining = Math.min(
                  distanceTo(bufindex, kvbidx) - 2 * METASIZE,
                  softLimit - bUsed) - METASIZE;
              continue;
            } else if (bufsoftlimit && kvindex != kvend) {
              // spill records, if any collected; check latter, as it may
              // be possible for metadata alignment to hit spill pcnt
              startSpill();
              final int avgRec = (int)
                (mapOutputByteCounter.getCounter() /
                mapOutputRecordCounter.getCounter());
              // leave at least half the split buffer for serialization data
              // ensure that kvindex >= bufindex
              final int distkvi = distanceTo(bufindex, kvbidx);
              final int newPos = (bufindex +
                Math.max(2 * METASIZE - 1,
                        Math.min(distkvi / 2,
                                 distkvi / (METASIZE + avgRec) * METASIZE)))
                % kvbuffer.length;
              setEquator(newPos);
              bufmark = bufindex = newPos;
              final int serBound = 4 * kvend;
              // bytes remaining before the lock must be held and limits
              // checked is the minimum of three arcs: the metadata space, the
              // serialization space, and the soft limit
              bufferRemaining = Math.min(
                  // metadata max
                  distanceTo(bufend, newPos),
                  Math.min(
                    // serialization max
                    distanceTo(newPos, serBound),
                    // soft limit
                    softLimit)) - 2 * METASIZE;
            }
          }
        } while (false);
      } finally {
        spillLock.unlock();
      }
    }

    try {
      // serialize key bytes into buffer
      int keystart = bufindex;
      keySerializer.serialize(key);
      if (bufindex < keystart) {
        // wrapped the key; must make contiguous
        bb.shiftBufferedKey();
        keystart = 0;
      }
      // serialize value bytes into buffer
      final int valstart = bufindex;
      valSerializer.serialize(value);
      // It's possible for records to have zero length, i.e. the serializer
      // will perform no writes. To ensure that the boundary conditions are
      // checked and that the kvindex invariant is maintained, perform a
      // zero-length write into the buffer. The logic monitoring this could be
      // moved into collect, but this is cleaner and inexpensive. For now, it
      // is acceptable.
      bb.write(b0, 0, 0);

      // the record must be marked after the preceding write, as the metadata
      // for this record are not yet written
      int valend = bb.markRecord();

      mapOutputRecordCounter.increment(1);
      mapOutputByteCounter.increment(
          distanceTo(keystart, valend, bufvoid));

      // write accounting info
      kvmeta.put(kvindex + PARTITION, partition);
      kvmeta.put(kvindex + KEYSTART, keystart);
      kvmeta.put(kvindex + VALSTART, valstart);
      kvmeta.put(kvindex + VALLEN, distanceTo(valstart, valend));
      // advance kvindex
      kvindex = (kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity();
    } catch (MapBufferTooSmallException e) {
      LOG.info("Record too large for in-memory buffer: " + e.getMessage());
      spillSingleRecord(key, value, partition);
      mapOutputRecordCounter.increment(1);
      return;
    }
  }

  private TaskAttemptID getTaskID() {
    return mapTask.getTaskID();
  }

	private void writeEmptyOutput(FSDataOutputStream dataOut, FSDataOutputStream indexOut) throws IOException {
		//create dummy output
		for (int i = 0; i < partitions; i++) {
			long segmentStart = dataOut.getPos();
			IFile.Writer<K, V> writer = new IFile.Writer<K, V>(job, dataOut, keyClass, valClass, codec, null);
			writer.close();
			writeIndexRecord(indexOut, dataOut, segmentStart, writer);
		}
	}

  /**
   * Set the point from which meta and serialization data expand. The meta
   * indices are aligned with the buffer, so metadata never spans the ends of
   * the circular buffer.
   */
  private void setEquator(int pos) {
    equator = pos;
    // set index prior to first entry, aligned at meta boundary
    final int aligned = pos - (pos % METASIZE);
    kvindex =
      ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    if (LOG.isInfoEnabled()) {
      LOG.info("(EQUATOR) " + pos + " kvi " + kvindex +
          "(" + (kvindex * 4) + ")");
    }
  }

  /**
   * The spill is complete, so set the buffer and meta indices to be equal to
   * the new equator to free space for continuing collection. Note that when
   * kvindex == kvend == kvstart, the buffer is empty.
   */
  private void resetSpill() {
    final int e = equator;
    bufstart = bufend = e;
    final int aligned = e - (e % METASIZE);
    // set start/end to point to first meta record
    kvstart = kvend =
      ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    if (LOG.isInfoEnabled()) {
      LOG.info("(RESET) equator " + e + " kv " + kvstart + "(" +
        (kvstart * 4) + ")" + " kvi " + kvindex + "(" + (kvindex * 4) + ")");
    }
  }

  /**
   * Compute the distance in bytes between two indices in the serialization
   * buffer.
   * @see #distanceTo(int,int,int)
   */
  final int distanceTo(final int i, final int j) {
    return distanceTo(i, j, kvbuffer.length);
  }

  /**
   * Compute the distance between two indices in the circular buffer given the
   * max distance.
   */
  int distanceTo(final int i, final int j, final int mod) {
    return i <= j
      ? j - i
      : mod - i + j;
  }

  /**
   * For the given meta position, return the offset into the int-sized
   * kvmeta buffer.
   */
  int offsetFor(int metapos) {
    return metapos * NMETA;
  }

  /**
   * Compare logical range, st i, j MOD offset capacity.
   * Compare by partition, then by key.
   * @see IndexedSortable#compare
   */
  public int compare(final int mi, final int mj) {
    final int kvi = offsetFor(mi % maxRec);
    final int kvj = offsetFor(mj % maxRec);
    final int kvip = kvmeta.get(kvi + PARTITION);
    final int kvjp = kvmeta.get(kvj + PARTITION);
    // sort by partition
    if (kvip != kvjp) {
      return kvip - kvjp;
    }
    // sort by key
    return comparator.compare(kvbuffer,
        kvmeta.get(kvi + KEYSTART),
        kvmeta.get(kvi + VALSTART) - kvmeta.get(kvi + KEYSTART),
        kvbuffer,
        kvmeta.get(kvj + KEYSTART),
        kvmeta.get(kvj + VALSTART) - kvmeta.get(kvj + KEYSTART));
  }

  final byte META_BUFFER_TMP[] = new byte[METASIZE];
  /**
   * Swap metadata for items i, j
   * @see IndexedSortable#swap
   */
  public void swap(final int mi, final int mj) {
    int iOff = (mi % maxRec) * METASIZE;
    int jOff = (mj % maxRec) * METASIZE;
    System.arraycopy(kvbuffer, iOff, META_BUFFER_TMP, 0, METASIZE);
    System.arraycopy(kvbuffer, jOff, kvbuffer, iOff, METASIZE);
    System.arraycopy(META_BUFFER_TMP, 0, kvbuffer, jOff, METASIZE);
  }

  /**
   * Inner class managing the spill of serialized records to disk.
   */
  protected class BlockingBuffer extends DataOutputStream {

    public BlockingBuffer() {
      super(new Buffer());
    }

    /**
     * Mark end of record. Note that this is required if the buffer is to
     * cut the spill in the proper place.
     */
    public int markRecord() {
      bufmark = bufindex;
      return bufindex;
    }

    /**
     * Set position from last mark to end of writable buffer, then rewrite
     * the data between last mark and kvindex.
     * This handles a special case where the key wraps around the buffer.
     * If the key is to be passed to a RawComparator, then it must be
     * contiguous in the buffer. This recopies the data in the buffer back
     * into itself, but starting at the beginning of the buffer. Note that
     * this method should <b>only</b> be called immediately after detecting
     * this condition. To call it at any other time is undefined and would
     * likely result in data loss or corruption.
     * @see #markRecord()
     */
    protected void shiftBufferedKey() throws IOException {
      // spillLock unnecessary; both kvend and kvindex are current
      int headbytelen = bufvoid - bufmark;
      bufvoid = bufmark;
      final int kvbidx = 4 * kvindex;
      final int kvbend = 4 * kvend;
      final int avail =
        Math.min(distanceTo(0, kvbidx), distanceTo(0, kvbend));
      if (bufindex + headbytelen < avail) {
        System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
        System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
        bufindex += headbytelen;
        bufferRemaining -= kvbuffer.length - bufvoid;
      } else {
        byte[] keytmp = new byte[bufindex];
        System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
        bufindex = 0;
        out.write(kvbuffer, bufmark, headbytelen);
        out.write(keytmp);
      }
    }
  }

  public class Buffer extends OutputStream {
    private final byte[] scratch = new byte[1];

    @Override
    public void write(int v)
        throws IOException {
      scratch[0] = (byte)v;
      write(scratch, 0, 1);
    }

    /**
     * Attempt to write a sequence of bytes to the collection buffer.
     * This method will block if the spill thread is running and it
     * cannot write.
     * @throws MapBufferTooSmallException if record is too large to
     *    deserialize into the collection buffer.
     */
    @Override
    public void write(byte b[], int off, int len)
        throws IOException {
      // must always verify the invariant that at least METASIZE bytes are
      // available beyond kvindex, even when len == 0
      bufferRemaining -= len;
      if (bufferRemaining <= 0) {
        // writing these bytes could exhaust available buffer space or fill
        // the buffer to soft limit. check if spill or blocking are necessary
        boolean blockwrite = false;
        spillLock.lock();
        try {
          do {
            checkSpillException();

            final int kvbidx = 4 * kvindex;
            final int kvbend = 4 * kvend;
            // ser distance to key index
            final int distkvi = distanceTo(bufindex, kvbidx);
            // ser distance to spill end index
            final int distkve = distanceTo(bufindex, kvbend);

            // if kvindex is closer than kvend, then a spill is neither in
            // progress nor complete and reset since the lock was held. The
            // write should block only if there is insufficient space to
            // complete the current write, write the metadata for this record,
            // and write the metadata for the next record. If kvend is closer,
            // then the write should block if there is too little space for
            // either the metadata or the current write. Note that collect
            // ensures its metadata requirement with a zero-length write
            blockwrite = distkvi <= distkve
              ? distkvi <= len + 2 * METASIZE
              : distkve <= len || distanceTo(bufend, kvbidx) < 2 * METASIZE;

            if (!spillInProgress) {
              if (blockwrite) {
                if ((kvbend + METASIZE) % kvbuffer.length !=
                    equator - (equator % METASIZE)) {
                  // spill finished, reclaim space
                  // need to use meta exclusively; zero-len rec & 100% spill
                  // pcnt would fail
                  resetSpill(); // resetSpill doesn't move bufindex, kvindex
                  bufferRemaining = Math.min(
                      distkvi - 2 * METASIZE,
                      softLimit - distanceTo(kvbidx, bufindex)) - len;
                  continue;
                }
                // we have records we can spill; only spill if blocked
                if (kvindex != kvend) {
                  startSpill();
                  // Blocked on this write, waiting for the spill just
                  // initiated to finish. Instead of repositioning the marker
                  // and copying the partial record, we set the record start
                  // to be the new equator
                  setEquator(bufmark);
                } else {
                  // We have no buffered records, and this record is too large
                  // to write into kvbuffer. We must spill it directly from
                  // collect
                  final int size = distanceTo(bufstart, bufindex) + len;
                  setEquator(0);
                  bufstart = bufend = bufindex = equator;
                  kvstart = kvend = kvindex;
                  bufvoid = kvbuffer.length;
                  throw new MapBufferTooSmallException(size + " bytes");
                }
              }
            }

            if (blockwrite) {
              // wait for spill
              try {
                while (spillInProgress) {
                  reporter.progress();
                  spillDone.await();
                }
              } catch (InterruptedException e) {
                  throw new IOException(
                      "Buffer interrupted while waiting for the writer", e);
              }
            }
          } while (blockwrite);
        } finally {
          spillLock.unlock();
        }
      }
      // here, we know that we have sufficient space to write
      if (bufindex + len > bufvoid) {
        final int gaplen = bufvoid - bufindex;
        System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
        len -= gaplen;
        off += gaplen;
        bufindex = 0;
      }
      System.arraycopy(b, off, kvbuffer, bufindex, len);
      bufindex += len;
    }

  }

  private void writeIndexRecord(FSDataOutputStream indexOut,
      FSDataOutputStream out, long start,
      IFile.Writer<K, V> writer)
  throws IOException {
    //when we write the offset/decompressed-length/compressed-length to
    //the final index file, we write longs for both compressed and
    //decompressed lengths. This helps us to reliably seek directly to
    //the offset/length for a partition when we start serving the
    //byte-ranges to the reduces. We probably waste some space in the
    //file by doing this as opposed to writing VLong but it helps us later on.
    // index record: <offset, raw-length, compressed-length>
    //StringBuffer sb = new StringBuffer();
    indexOut.writeLong(start);
    indexOut.writeLong(writer.getRawLength());
    long segmentLength = out.getPos() - start;
    indexOut.writeLong(segmentLength);
	
    LOG.debug("index record <offset, raw-length, compressed-length>: " +
        start + ", " + writer.getRawLength() + ", " + segmentLength);
  }

  public void flush() throws IOException, ClassNotFoundException,
         InterruptedException {
    LOG.info("Starting flush of map output");
    spillLock.lock();
    try {
      while (spillInProgress) {
        reporter.progress();
        spillDone.await();
      }
      checkSpillException();

      final int kvbend = 4 * kvend;
      if ((kvbend + METASIZE) % kvbuffer.length !=
          equator - (equator % METASIZE)) {
        // spill finished
        resetSpill();
      }
      if (kvindex != kvend) {
        kvend = (kvindex + NMETA) % kvmeta.capacity();
        bufend = bufmark;
        if (LOG.isInfoEnabled()) {
          LOG.info("Spilling map output");
          LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                   "; bufvoid = " + bufvoid);
          LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                   "); kvend = " + kvend + "(" + (kvend * 4) +
                   "); length = " + (distanceTo(kvend, kvstart,
                         kvmeta.capacity()) + 1) + "/" + maxRec);
        }
        sortAndSpill();
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for the writer", e);
    } finally {
      spillLock.unlock();
    }
    assert !spillLock.isHeldByCurrentThread();
    // shut down spill thread and wait for it to exit. Since the preceding
    // ensures that it is finished with its work (and sortAndSpill did not
    // throw), we elect to use an interrupt instead of setting a flag.
    // Spilling simultaneously from this thread while the spill thread
    // finishes its work might be both a useful way to extend this and also
    // sufficient motivation for the latter approach.
    try {
      spillThread.interrupt();
      spillThread.join();
    } catch (InterruptedException e) {
      throw new IOException("Spill failed", e);
    }
    // release sort buffer before the merge
    kvbuffer = null;
    mergeParts();
    Path outputPath = mapOutputFile.getOutputFile();
    fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
  }

    /*
  public void close() throws IOException {
    //if (this.open == false) return;
    synchronized (spillLock) {
      //this.open = false;
      while (isSpilling()) {
        try { spillLock.wait();
        } catch (InterruptedException e) { }
      }
      /*/

      //boolean spill = forceSpill(); // flush what remains
      // sortAndSpill(); // flush what remains
        /*
      if (pipeline) {
        // TODO fix this later if we want to add pipelining back in
        if (!spill) {
          // we must create a sentinal spill with progress == 1f
          LOG.debug("SpillThread: create sentinel spill file for pipelining.");
          int dataSize = partitions * APPROX_HEADER_LENGTH;
          Path data = outputHandle.getSpillFileForWrite(taskid, spills.size(), dataSize);
          FSDataOutputStream dataOut = rfs.create(data, false);
          Path index = outputHandle.getSpillIndexFileForWrite(
              taskid, spills.size(), partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
          FSDataOutputStream indexOut = rfs.create(index, false);
          writeEmptyOutput(dataOut, indexOut);
          dataOut.close(); indexOut.close();
          PartitionBufferFile spillFile = new PartitionBufferFile(spills.size(), data, index, 1f, true);
          LOG.debug("Finished spill sentinal. id = " + spills.size());
          spills.add(spillFile);
        }
        //pipeline();
      }
      spillLock.notifyAll();
    }
  }
          */


	protected class InMemValBytes extends DataInputBuffer {
		private byte[] buffer;
		private int start;
		private int length;

		public void reset(byte[] buffer, int start, int length) {
			this.buffer = buffer;
			this.start = start;
			this.length = length;

			if (start + length > bufvoid) {
				this.buffer = new byte[this.length];
				final int taillen = bufvoid - start;
				System.arraycopy(buffer, start, this.buffer, 0, taillen);
				System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
				this.start = 0;
			}

			super.reset(this.buffer, this.start, this.length);
		}
	}

  protected class SpillThread extends Thread {

    @Override
    public void run() {
      spillLock.lock();
      spillThreadRunning = true;
      try {
        while (true) {
          spillDone.signal();
          while (!spillInProgress) {
            spillReady.await();
          }
          try {
            spillLock.unlock();
            sortAndSpill();
          } catch (Throwable t) {
            sortSpillException = t;
          } finally {
            spillLock.lock();
            if (bufend < bufstart) {
              bufvoid = kvbuffer.length;
            }
            kvstart = kvend;
            bufstart = bufend;
            spillInProgress = false;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        spillLock.unlock();
        spillThreadRunning = false;
      }
    }


  }

  private void checkSpillException() throws IOException {
    final Throwable lspillException = sortSpillException;
    if (lspillException != null) {
      if (lspillException instanceof Error) {
        final String logMsg = "Task " + getTaskID() + " failed : " +
          StringUtils.stringifyException(lspillException);
        mapTask.reportFatalError(getTaskID(), lspillException, logMsg);
      }
      throw new IOException("Spill failed", lspillException);
    }
  }

  private void startSpill() {
    assert !spillInProgress;
    kvend = (kvindex + NMETA) % kvmeta.capacity();
    bufend = bufmark;
    spillInProgress = true;
    if (LOG.isInfoEnabled()) {
      LOG.info("Spilling map output");
      LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
               "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
               "); kvend = " + kvend + "(" + (kvend * 4) +
               "); length = " + (distanceTo(kvend, kvstart,
                     kvmeta.capacity()) + 1) + "/" + maxRec);
    }
    spillReady.signal();
  }

  private void sortAndSpill() throws IOException, ClassNotFoundException,
                                     InterruptedException {
    //approximate the length of the output file to be the length of the
    //buffer + header lengths for the partitions
    final long size = (bufend >= bufstart
        ? bufend - bufstart
        : (bufvoid - bufend) + bufstart) +
                partitions * MapTask.APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    try {
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions);
      final Path filename =
          mapOutputFile.getSpillFileForWrite(numSpills, size);
      out = rfs.create(filename);

      final int mstart = kvend / NMETA;
      final int mend = 1 + // kvend is a valid record
        (kvstart >= kvend
        ? kvstart
        : kvmeta.capacity() + kvstart) / NMETA;
      sorter.sort(InMemMapOutputBuffer.this, mstart, mend, reporter);
      int spindex = mstart;
      final IndexRecord rec = new IndexRecord();
      final InMemValBytes value = new InMemValBytes();
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer<K, V> writer = null;
        try {
          long segmentStart = out.getPos();
          writer = new Writer<K, V>(job, out, keyClass, valClass, codec,
                                    spilledRecordsCounter);
          if (combinerRunner == null) {
            // spill directly
            DataInputBuffer key = new DataInputBuffer();
            while (spindex < mend &&
                kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
              final int kvoff = offsetFor(spindex % maxRec);
              int keystart = kvmeta.get(kvoff + KEYSTART);
              int valstart = kvmeta.get(kvoff + VALSTART);
              key.reset(kvbuffer, keystart, valstart - keystart);
              getVBytesForOffset(kvoff, value);
              writer.append(key, value);
              ++spindex;
            }
          } else {
            int spstart = spindex;
            while (spindex < mend &&
                kvmeta.get(offsetFor(spindex % maxRec)
                          + PARTITION) == i) {
              ++spindex;
            }
            // Note: we would like to avoid the combiner if we've fewer
            // than some threshold of records for a partition
            if (spstart != spindex) {
              combineCollector.setWriter(writer);
              RawKeyValueIterator kvIter =
                new MRResultIterator(spstart, spindex);
              combinerRunner.combine(kvIter, combineCollector);
            }
          }

          // close the writer
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillRec.putIndex(rec, i);

          writer = null;
        } finally {
          if (null != writer) writer.close();
        }
      }

      if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
        // create spill index file
        Path indexFilename =
            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, job);
        PartitionBufferFile spill = new PartitionBufferFile(spills.size(), filename, indexFilename, progress.get(), this.eof);
        spills.add(spill);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      LOG.info("Finished spill " + numSpills);
      ++numSpills;
    } finally {
      if (out != null) out.close();
    }
  }

  /**
   * Handles the degenerate case where serialization fails to fit in
   * the in-memory buffer, so we must spill the record from collect
   * directly to a spill file. Consider this "losing".
   */
  private void spillSingleRecord(final K key, final V value,
                                 int partition) throws IOException {
    long size = kvbuffer.length + partitions * MapTask.APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    try {
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions);
      final Path filename =
          mapOutputFile.getSpillFileForWrite(numSpills, size);
      out = rfs.create(filename);

      // we don't run the combiner for a single record
      IndexRecord rec = new IndexRecord();
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer<K, V> writer = null;
        try {
          long segmentStart = out.getPos();
          // Create a new codec, don't care!
          writer = new IFile.Writer<K,V>(job, out, keyClass, valClass, codec,
                                          spilledRecordsCounter);

          if (i == partition) {
            final long recordStart = out.getPos();
            writer.append(key, value);
            // Note that our map byte count will not be accurate with
            // compression
            mapOutputByteCounter.increment(out.getPos() - recordStart);
          }
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillRec.putIndex(rec, i);

          writer = null;
        } catch (IOException e) {
          if (null != writer) writer.close();
          throw e;
        }
      }
      if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
        // create spill index file
        Path indexFilename =
            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, job);
        spills.add(new PartitionBufferFile(spills.size(), filename, indexFilename, progress.get(), this.eof)); //-jbuck
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      ++numSpills;
    } finally {
      if (out != null) out.close();
    }
  }

  /**
   * Given an offset, populate vbytes with the associated set of
   * deserialized value bytes. Should only be called during a spill.
   */
  private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
    // get the keystart for the next serialized value to be the end
    // of this value. If this is the last value in the buffer, use bufend
    final int vallen = kvmeta.get(kvoff + VALLEN);
    assert vallen >= 0;
    vbytes.reset(kvbuffer, kvmeta.get(kvoff + VALSTART), vallen);
  }

  protected class MRResultIterator implements RawKeyValueIterator {
    private final DataInputBuffer keybuf = new DataInputBuffer();
    private final InMemValBytes vbytes = new InMemValBytes();
    private final int end;
    private int current;
    public MRResultIterator(int start, int end) {
      this.end = end;
      current = start - 1;
    }
    public boolean next() throws IOException {
      return ++current < end;
    }
    public DataInputBuffer getKey() throws IOException {
      final int kvoff = offsetFor(current % maxRec);
      keybuf.reset(kvbuffer, kvmeta.get(kvoff + KEYSTART),
          kvmeta.get(kvoff + VALSTART) - kvmeta.get(kvoff + KEYSTART));
      return keybuf;
    }
    public DataInputBuffer getValue() throws IOException {
      getVBytesForOffset(offsetFor(current % maxRec), vbytes);
      return vbytes;
    }
    public Progress getProgress() {
      return null;
    }
    public void close() { }
  }

  private void mergeParts() throws IOException, InterruptedException, 
                                   ClassNotFoundException {
    // get the approximate size of the final output/index files
    long finalOutFileSize = 0;
    long finalIndexFileSize = 0;
    final Path[] filename = new Path[numSpills];
    final TaskAttemptID mapId = getTaskID();

    for(int i = 0; i < numSpills; i++) {
      filename[i] = mapOutputFile.getSpillFile(i);
      finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
    }
    if (numSpills == 1) { //the spill is the final output
      sameVolRename(filename[0],
          mapOutputFile.getOutputFileForWriteInVolume(filename[0]));
      if (indexCacheList.size() == 0) {
        sameVolRename(mapOutputFile.getSpillIndexFile(0),
          mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]));
      } else {
        indexCacheList.get(0).writeToFile(
          mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]), job);
      }
      sortPhase.complete();
      return;
    }

    // read in paged indices
    for (int i = indexCacheList.size(); i < numSpills; ++i) {
      Path indexFileName = mapOutputFile.getSpillIndexFile(i);
      indexCacheList.add(new SpillRecord(indexFileName, job));
    }

    //make correction in the length to include the sequence file header
    //lengths for each partition
    finalOutFileSize += partitions * MapTask.APPROX_HEADER_LENGTH;
    finalIndexFileSize = partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
    Path finalOutputFile =
        mapOutputFile.getOutputFileForWrite(finalOutFileSize);
    Path finalIndexFile =
        mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

    //The output stream for the final single output file
    FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

    if (numSpills == 0) {
      //create dummy files
      IndexRecord rec = new IndexRecord();
      SpillRecord sr = new SpillRecord(partitions);
      try {
        for (int i = 0; i < partitions; i++) {
          long segmentStart = finalOut.getPos();
          Writer<K, V> writer =
            new Writer<K, V>(job, finalOut, keyClass, valClass, codec, null);
          writer.close();
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          sr.putIndex(rec, i);
        }
        sr.writeToFile(finalIndexFile, job);
      } finally {
        finalOut.close();
      }
      sortPhase.complete();
      return;
    }
    {
      sortPhase.addPhases(partitions); // Divide sort phase into sub-phases
      Merger.considerFinalMergeForProgress();
      
      IndexRecord rec = new IndexRecord();
      final SpillRecord spillRec = new SpillRecord(partitions);
      for (int parts = 0; parts < partitions; parts++) {
        //create the segments to be merged
        List<Segment<K,V>> segmentList =
          new ArrayList<Segment<K, V>>(numSpills);
        for(int i = 0; i < numSpills; i++) {
          IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

          Segment<K,V> s =
            new Segment<K,V>(job, rfs, filename[i], indexRecord.startOffset,
                             indexRecord.partLength, codec, true);
          segmentList.add(i, s);

          if (LOG.isDebugEnabled()) {
            LOG.debug("MapId=" + mapId + " Reducer=" + parts +
                "Spill =" + i + "(" + indexRecord.startOffset + "," +
                indexRecord.rawLength + ", " + indexRecord.partLength + ")");
          }
        }

        int mergeFactor = job.getInt(JobContext.IO_SORT_FACTOR, 100);
        // sort the segments only if there are intermediate merges
        boolean sortSegments = segmentList.size() > mergeFactor;
        //merge
        @SuppressWarnings("unchecked")
        RawKeyValueIterator kvIter = Merger.merge(job, rfs,
                       keyClass, valClass, codec,
                       segmentList, mergeFactor,
                       new Path(mapId.toString()),
                       job.getOutputKeyComparator(), reporter, sortSegments,
                       null, spilledRecordsCounter, sortPhase.phase());

        //write merged output to disk
        long segmentStart = finalOut.getPos();
        Writer<K, V> writer =
            new Writer<K, V>(job, finalOut, keyClass, valClass, codec,
                             spilledRecordsCounter);
        if (combinerRunner == null || numSpills < minSpillsForCombine) {
          Merger.writeFile(kvIter, writer, reporter, job);
        } else {
          combineCollector.setWriter(writer);
          combinerRunner.combine(kvIter, combineCollector);
        }

        //close
        writer.close();

        sortPhase.startNextPhase();
        
        // record offsets
        rec.startOffset = segmentStart;
        rec.rawLength = writer.getRawLength();
        rec.partLength = writer.getCompressedLength();
        spillRec.putIndex(rec, parts);
      }
      spillRec.writeToFile(finalIndexFile, job);
      finalOut.close();
      for(int i = 0; i < numSpills; i++) {
        rfs.delete(filename[i],true);
      }
    }
  }
  
  /**
   * Rename srcPath to dstPath on the same volume. This is the same
   * as RawLocalFileSystem's rename method, except that it will not
   * fall back to a copy, and it will create the target directory
   * if it doesn't exist.
   */
  private void sameVolRename(Path srcPath,
      Path dstPath) throws IOException {
    RawLocalFileSystem rfs = (RawLocalFileSystem)this.rfs;
    File src = rfs.pathToFile(srcPath);
    File dst = rfs.pathToFile(dstPath);
    if (!dst.getParentFile().exists()) {
      if (!dst.getParentFile().mkdirs()) {
        throw new IOException("Unable to rename " + src + " to "
            + dst + ": couldn't create parent directory"); 
      }
    }
    
    if (!src.renameTo(dst)) {
      throw new IOException("Unable to rename " + src + " to " + dst);
    }
  }

	/**
	 * Inner class managing the spill of serialized records to disk.
	 */

  /*
	protected class FSMRResultIterator implements RawKeyValueIterator {
		private IFile.Reader reader;
		private DataInputBuffer key = new DataInputBuffer();
		private DataInputBuffer value = new DataInputBuffer();
		private Progress progress = new Progress();

		public FSMRResultIterator(FileSystem localFS, Path path) throws IOException {
			this.reader = new IFile.Reader<K, V>(job, localFS, path, codec, null);
		}

		@Override
		public void close() throws IOException {
			this.reader.close();
		}

		@Override
		public DataInputBuffer getKey() throws IOException {
			return this.key;
		}

		@Override
		public Progress getProgress() {
			try {
				float score = reader.getPosition() / (float) reader.getLength();
				this.progress.set(score);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return this.progress;
		}

		@Override
		public DataInputBuffer getValue() throws IOException {
			return this.value;
		}

		@Override
		public boolean next() throws IOException {
			return this.reader.next(key, value);
		}
	}
  */

} 


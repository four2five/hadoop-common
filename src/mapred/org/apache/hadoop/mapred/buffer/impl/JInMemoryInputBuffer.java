package org.apache.hadoop.mapred.buffer.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
//import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileHandle;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.InputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.RamManager;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.IFile.InMemoryReader;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
//import org.apache.hadoop.mapred.buffer.OutputFile;
import org.apache.hadoop.mapred.buffer.OutputInMemoryBuffer;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.StringUtils;

public class JInMemoryInputBuffer<K extends Object, V extends Object>
extends Buffer<K, V> implements InputCollector<K, V> {
	private static final Log LOG = LogFactory.getLog(JInMemoryInputBuffer.class.getName());

	/**
	 * This class contains the methods that should be used for metrics-reporting
	 * the specific metrics for shuffle. This class actually reports the
	 * metrics for the shuffle client (the ReduceTask), and hence the name
	 * ShuffleClientMetrics.
	 */
	class ShuffleClientMetrics implements Updater {
		private MetricsRecord shuffleMetrics = null;
		private int numFailedFetches = 0;
		private int numSuccessFetches = 0;
		private long numBytes = 0;
		private int numThreadsBusy = 0;
		ShuffleClientMetrics(JobConf conf) {
			MetricsContext metricsContext = MetricsUtil.getContext("mapred");
			this.shuffleMetrics = 
				MetricsUtil.createRecord(metricsContext, "shuffleInput");
			this.shuffleMetrics.setTag("user", conf.getUser());
			this.shuffleMetrics.setTag("jobName", conf.getJobName());
			this.shuffleMetrics.setTag("jobId", task.getJobID().toString());
			this.shuffleMetrics.setTag("taskId", task.getTaskID().toString());
			this.shuffleMetrics.setTag("sessionId", conf.getSessionId());
			metricsContext.registerUpdater(this);
		}
		public synchronized void inputBytes(long numBytes) {
			this.numBytes += numBytes;
		}
		public synchronized void failedFetch() {
			++numFailedFetches;
		}
		public synchronized void successFetch() {
			++numSuccessFetches;
		}
		public synchronized void threadBusy() {
			++numThreadsBusy;
		}
		public synchronized void threadFree() {
			--numThreadsBusy;
		}
		public void doUpdates(MetricsContext unused) {
			synchronized (this) {
				shuffleMetrics.incrMetric("shuffle_input_bytes", numBytes);
				shuffleMetrics.incrMetric("shuffle_failed_fetches", 
						numFailedFetches);
				shuffleMetrics.incrMetric("shuffle_success_fetches", 
						numSuccessFetches);
				numBytes = 0;
				numSuccessFetches = 0;
				numFailedFetches = 0;
			}
			shuffleMetrics.update();
		}
	}

	/** Describes an input file; can only be in-memory. */
	private class JInput {
		//final TaskID taskid;
		final TaskAttemptID taskattemptid;

		//Path file;

		byte[] data;
		//boolean inMemory;
		long compressedSize;

		public JInput(TaskAttemptID taskattemptid, byte[] data, int compressedLength) {
			this.taskattemptid = taskattemptid;
			this.data = data;
			this.compressedSize = compressedLength;
		}

		public void discard() throws IOException {
      /*
			if (inMemory) {
				data = null;
			} else {
				localFileSys.delete(file, true);
			}
      */
			data = null;
		}

		public FileStatus status() {
			return null;
		}
	}

	class ShuffleRamManager implements RamManager {
		/* Maximum percentage of the in-memory limit that a single shuffle can 
		 * consume*/ 
		private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.99f;

		/* Maximum percentage of shuffle-threads which can be stalled 
		 * simultaneously after which a merge is triggered. */ 
		private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;

		private final int maxSize;
		private final int maxSingleShuffleLimit;

		private int size;

		private Object dataAvailable = new Object();
		private int fullSize; 
		private int numPendingRequests;
		private int numClosed;
		private boolean closed;

		public ShuffleRamManager(Configuration conf) throws IOException {
			final float maxInMemCopyUse =
				conf.getFloat("mapred.job.shuffle.input.buffer.percent", 0.70f);
			if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
				throw new IOException("mapred.job.shuffle.input.buffer.percent" +
						maxInMemCopyUse);
			}
			maxSize = (int)Math.min(
					Runtime.getRuntime().maxMemory() * maxInMemCopyUse,
					Integer.MAX_VALUE);
			maxSingleShuffleLimit = (int)(maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION);
			LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize + " ( " + (maxSize * 1000000f) + " mb)" +
					", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
			reset();
		}
		
		public void reset() {
			size = 0;
			fullSize = 0;
			numPendingRequests = 0;
			numClosed = 0;
			closed = false;
		}

		public synchronized boolean reserve(int requestedSize, InputStream in) { 
      LOG.info("In reserve. size: " + size +  
               " requestedSize: " + requestedSize +  
               " maxSize: " + maxSize);

			// Wait till the request can be fulfilled...
			while ((size + requestedSize) > maxSize) {

        // Close the input...
        if (in != null) {
          try {
            in.close();
          } catch (IOException ie) {
            LOG.error("Failed to close connection with: " + ie);
          } finally {
            in = null;
          }
        }
				// Track pending requests
				synchronized (dataAvailable) {
					++numPendingRequests;
					dataAvailable.notify();
				}

				// Wait for memory to free up
				try {
					wait();
				} catch (InterruptedException e) {
          LOG.error("IE in reserve: " + e.toString());
					return false;
				} finally {
					// Track pending requests
					synchronized (dataAvailable) {
						--numPendingRequests;
					}
				}
			}
			size += requestedSize;
			LOG.info("JInMemoryInputBuffer: reserve. size = " + size);
			return (in != null);
		}

		public synchronized void unreserve(int requestedSize) {
			size -= requestedSize;
			LOG.debug("JInMemoryInputBuffer: unreserve. size = " + size);

			synchronized (dataAvailable) {
				fullSize -= requestedSize;
				--numClosed;
			}

			// Notify the threads blocked on RamManager.reserve
			notifyAll();
		}

		public boolean waitForDataToMerge() throws InterruptedException {
			boolean done = false;
			synchronized (dataAvailable) {
				// Start in-memory merge if manager has been closed or...
				while (!closed
						&&
						// In-memory threshold exceeded and at least two segments
						// have been fetched
						(getPercentUsed() < maxInMemCopyPer || numClosed < 2)
						&&
						// More than "mapred.inmem.merge.threshold" map outputs
						// have been fetched into memory
						(maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)) {
          LOG.info("closed: " + closed + " getPercentUsed() " + getPercentUsed() +
                   " maxInMemCopyPer " + maxInMemCopyPer + " numClosed " + numClosed + 
                   " maxInMemOutputs " + maxInMemOutputs);
					dataAvailable.wait();
				}
				done = closed;
        LOG.info("closed: " + closed + " getPercentUsed() " + getPercentUsed() +
                 " maxInMemCopyPer " + maxInMemCopyPer + " numClosed " + numClosed + 
                 " maxInMemOutputs " + maxInMemOutputs);
			}
			return done;
		}

		public void closeInMemoryFile(int requestedSize) {
			synchronized (dataAvailable) {
				fullSize += requestedSize;
				++numClosed;
				LOG.debug("JInMemoryInputBuffer: closeInMemoryFile. percent used = " + getPercentUsed());
				dataAvailable.notify();
			}
		}

		public void close() {
			synchronized (dataAvailable) {
				closed = true;
				LOG.info("Closed ram manager");
				dataAvailable.notify();
			}
		}

		private float getPercentUsed() {
			return (float)fullSize/maxSize;
		}

		int getMemoryLimit() {
			return maxSize;
		}

		boolean canFitInMemory(long requestedSize) {
      boolean retVal =  (requestedSize < Integer.MAX_VALUE && 
					requestedSize < maxSingleShuffleLimit);
      LOG.info("canFitInMemory: " + retVal);
      return retVal;
		}
	}
	

	/* A reference to the RamManager for writing the map outputs to. */
	private ShuffleRamManager ramManager;

	/* A reference to the local file system for writing the map outputs to. */
	
	/* Number of files to merge at a time */
	private int ioSortFactor;

	private int spills;
	
	/**
	 * A reference to the throwable object (if merge throws an exception)
	 */
	private volatile Throwable mergeThrowable;

	/**
	 * When we accumulate maxInMemOutputs number of files in ram, we merge/spill
	 */
	private final int maxInMemOutputs;

	private final float maxInMemCopyPer;

	/**
	 * Maximum memory usage of map outputs to merge from memory into
	 * the reduce, in bytes.
	 */
	private final long maxInMemMerge;

	/**
	 * The object for metrics reporting.
	 */
	private ShuffleClientMetrics shuffleClientMetrics = null;

	/** 
	 * List of in-memory map-outputs.
	 */
	private final List<JInput> inputFilesInMemory =
		Collections.synchronizedList(new LinkedList<JInput>());

	// A custom comparator for map output files. Here the ordering is determined
	// by the file's size and path. In case of files with same size and different
	// file paths, the first parameter is considered smaller than the second one.
	// In case of files with same size and path are considered equal.
  /*
	private Comparator<JInput> inputFileComparator = 
		new Comparator<JInput>() {
		public int compare(JInput a, JInput b) {
			FileStatus astat = a.status();
			FileStatus bstat = b.status();
			if (astat.getLen() < bstat.getLen())
				return -1;
			else if (astat.getLen() == bstat.getLen())
				return a.file.toString().compareTo(b.file.toString());
			else
				return 1;
		}
	};
  */

	// A sorted set for keeping a set of map output files on disk
	//private final SortedSet<JInput> inputFilesOnDisk = 
	//	new TreeSet<JInput>(inputFileComparator);

	//private FileHandle outputHandle = null;


    //private LocalFSMerger localFSMergerThread = null;
    private InMemFSMergeThread inMemFSMergeThread = null;
    
    private boolean open = true;


    public JInMemoryInputBuffer(JobConf conf, Task task, 
    		Reporter reporter, Progress progress,
    		Class<K> keyClass, Class<V> valClass, 
    		Class<? extends CompressionCodec> codecClass)
    throws IOException {
    	super(conf, task, reporter, progress, keyClass, valClass, codecClass);
    	configureClasspath(conf);
    	this.spills = 0;

    	this.shuffleClientMetrics = new ShuffleClientMetrics(conf);
    	//this.outputHandle = new FileHandle(task.getJobID());
    	//this.outputHandle.setConf(conf);

    	this.ioSortFactor = conf.getInt("io.sort.factor", 10);
    	this.maxInMemOutputs = conf.getInt("mapred.inmem.merge.threshold", 1000);
    	this.maxInMemCopyPer = conf.getFloat("mapred.job.shuffle.merge.percent", 0.66f);

    	float maxRedPer = conf.getFloat("mapred.job.reduce.input.buffer.percent", 0f);
    	if (maxRedPer > 1.0 || maxRedPer < 0.0) {
    		throw new IOException("mapred.job.reduce.input.buffer.percent" +
    				maxRedPer);
    	}
    	this.maxInMemMerge = (int)Math.min(
    			Runtime.getRuntime().maxMemory() * maxRedPer, Integer.MAX_VALUE);

    	// Setup the RamManager
    	ramManager = new ShuffleRamManager(conf);

    	//start the in memory merger thread
    	inMemFSMergeThread = new InMemFSMergeThread();
    	inMemFSMergeThread.start();
    }

    // TODO --jbuck pick up here
    
	@Override
	public void close() {
		this.open = false;
		this.ramManager.close();
		//this.localFSMergerThread.interrupt();
		
		try {
			//this.localFSMergerThread.join();
			this.inMemFSMergeThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public synchronized void free() {
		ramManager.reset();
		
		for (JInput memInput : inputFilesInMemory) {
			try {
				memInput.discard();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		inputFilesInMemory.clear();
	}
	
	public void flush() throws IOException {
		flush(0); // Perform full flush
	}
	
	private void flush(long leaveBytes) throws IOException {
		List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K,V>>();
		long mergeOutputSize = 0;
		TaskAttemptID taskattemptid = null;
		
		synchronized (inputFilesInMemory) {
			if (inputFilesInMemory.size() == 0) {
				return;
			}

			//name this output file same as the name of the first file that is 
			//there in the current list of inmem files (this is guaranteed to
			//be absent on the disk currently. So we don't overwrite a prev. 
			//created spill). Also we need to create the output file now since
			//it is not guaranteed that this file will be present after merge
			//is called (we delete empty files as soon as we see them
			//in the merge method)

			//figure out the taskid that generated this input 
			//taskid = inputFilesInMemory.get(0).taskid;
			taskattemptid = inputFilesInMemory.get(0).taskattemptid;
			mergeOutputSize = createInMemorySegments(inMemorySegments, leaveBytes);
		}

		//Path outputPath = outputHandle.getInputFileForWrite(task.getTaskID(), taskattemptid.getTaskID(), spills++, mergeOutputSize);

    //ByteArrayOutputStream baIndexOut = new ByteArrayOutputStream(partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH);
    //DataOutputStream indexOut = new DataOutputStream(baIndexOut);
    LOG.info("Allocating a BAOS size " + (int)mergeOutputSize);
    boolean successfullyAllocated = false;
    int allocateCounter = 0;
    ByteArrayOutputStream baOut = null;
    while (!successfullyAllocated && allocateCounter < 5) { 
      successfullyAllocated = true;
      try {
        LOG.info("\t\t Allocating attempt " + allocateCounter);
        baOut =  new ByteArrayOutputStream((int)mergeOutputSize);
      } catch (OutOfMemoryError oome) { 
        successfullyAllocated = false;
        LOG.info("!!!! JB, supressing OOME: " + oome.toString());
        try {
          Thread.sleep(2000); // if the allocate fails, sleep for 2 seconds and then try again
        } catch (InterruptedException ie){
          LOG.error("Surpressing an InterruptedException");
        }
      }
      allocateCounter++;
    }

    if (null == baOut) { 
      throw new OutOfMemoryError("Failed to allocate a ByteArrayOutputStream after " + allocateCounter + " attempts");
    }

    DataOutputStream out = new DataOutputStream(baOut);

		IFile.Writer writer = 
			new IFile.Writer(conf, out,
					keyClass, valClass, codec, null);

		RawKeyValueIterator rIter = null;
		try {
			int segments = inMemorySegments.size();
			LOG.info("Initiating in-memory merge with " + 
					segments +
					" segments... total size = " + mergeOutputSize);


			rIter = Merger.merge(conf, null,
					keyClass, valClass,
					inMemorySegments, inMemorySegments.size(),
          null,
					//new Path(task.getTaskID().toString()),
					conf.getOutputKeyComparator(), reporter,
					null, null);

			if (null == combinerClass) {
				Merger.writeFile(rIter, writer, reporter, conf);
			} else {
				CombineOutputCollector combineCollector = new CombineOutputCollector();
				combineCollector.setWriter(writer);
				combineAndSpill(combineCollector, rIter);
			}
			writer.close();
			
			LOG.info(task.getTaskID() + 
					" Merge of the " + segments +
					" files in-memory complete.");
			
			//inMemorySegments.clear();
		} catch (Exception e) { 
			//make sure that we delete the ondisk file that we created 
			//earlier when we invoked cloneFileAttributes
			throw (IOException)new IOException
			("Intermediate merge failed").initCause(e);
		}

		LOG.info("FLUSH: Merged " + inMemorySegments.size() + " segments, merged " +
				mergeOutputSize + " bytes to satisfy reduce memory limit");
	}
	
	@Override
	public ValuesIterator<K, V> valuesIterator() throws IOException {
		RawKeyValueIterator kvIter = this.createKVIterator(conf, reporter);
		return new ValuesIterator<K, V>(kvIter, comparator, keyClass, valClass, conf, reporter);
	}
	
	@Override
	public synchronized boolean read(DataInputStream istream, OutputInMemoryBuffer.Header header)
	throws IOException {
    LOG.info("in read()");
    if (null == istream) { 
      LOG.error("in read(), istream is NULL. Bad news");
    }
		//TaskID taskid = header.owner().getTaskID();
		TaskAttemptID taskattemptid = header.owner();
		long compressedLength = header.compressed();
		long decompressedLength = header.decompressed();
		
		if (compressedLength < 0 || decompressedLength < 0) {
			LOG.warn("JBuffer: invalid lengths in map output header: id: " +
					taskattemptid + " compressed len: " + compressedLength +
					", decompressed len: " + decompressedLength);
			return false;
		}

		//We will put a file in memory if it meets certain criteria:
		//1. The size of the (decompressed) file should be less than 25% of 
		//    the total inmem fs
		//2. There is space available in the inmem fs

		// Check if this map-output can be saved in-memory
		boolean shuffleInMemory = ramManager.canFitInMemory(decompressedLength); 


		// Shuffle
		if (shuffleInMemory ) {  

      boolean successfulShuffle = shuffleInMemory(taskattemptid, istream,
					                  (int)decompressedLength,
					                  (int)compressedLength); 

      if ( successfulShuffle) { 
			  LOG.info("Shuffeled " + decompressedLength + " bytes (" + 
					  compressedLength + ") raw bytes");  
      } else  {
			  LOG.error("Shuffle failed for header " + taskattemptid);
      }
		} else {
      LOG.error("Cannot shuffle " + decompressedLength + " in memory. This is bad.");
    }

		return true;
	}
	
	private boolean shuffleInMemory(
			TaskAttemptID taskattemptid,
			InputStream ins,
			int decompressedLength,
			int compressedLength)
	throws IOException {

    if (null == ins) { 
      LOG.error("In shuffleInMemory and the passed in ins is null. Bad monkey");
    }
		// Reserve ram for the map-output
		boolean createdNow = ramManager.reserve(decompressedLength, ins);
		
		if (!createdNow) {
      LOG.error("Reserve failed, returning false from shuffleInMemory");
			return false;
		}

		IFileInputStream checksumIn = new IFileInputStream(ins, compressedLength);
		ins = checksumIn;       

		// Are map-outputs compressed?
		if (codec != null) {
			decompressor.reset();
			ins = codec.createInputStream(ins, decompressor);
		}

		LOG.info("JInMemoryInputBuffer input: copy compressed " + compressedLength + 
				" (decompressed " + decompressedLength + ") bytes from map " + taskattemptid);
		// Copy map-output into an in-memory buffer
		byte[] shuffleData = new byte[decompressedLength];
		JInput input = new JInput(taskattemptid, shuffleData, decompressedLength);

    long startTime = System.nanoTime();
		int bytesRead = 0;
		try {
			int n = ins.read(shuffleData, 0, shuffleData.length);
			while (n > 0) {
				bytesRead += n;
				shuffleClientMetrics.inputBytes(n);

				// indicate we're making progress
				reporter.progress();
				n = ins.read(shuffleData, bytesRead, shuffleData.length-bytesRead);
			}
      long totalTime = System.nanoTime() - startTime;
			LOG.info("Shuffle time " + totalTime + " read " + bytesRead + 
               " bytes from map-output for " + taskattemptid + 
               " inmem2");
		} catch (OutOfMemoryError oome) {
			LOG.info("Failed to shuffle from " + taskattemptid, 
					oome);

			// Inform the ram-manager
			ramManager.closeInMemoryFile(decompressedLength);
			ramManager.unreserve(decompressedLength);

			// Discard the map-output
			try {
				input.discard();
			} catch (IOException ignored) {
				LOG.error("Failed to discard map-output from " + taskattemptid, 
						ignored);
			}
			input = null;

			// Close the streams
			IOUtils.cleanup(LOG, ins);

			// Re-throw
			throw oome;

		} catch (IOException ioe) {
			LOG.info("Failed to shuffle from " + taskattemptid, 
					ioe);

			// Inform the ram-manager
			ramManager.closeInMemoryFile(decompressedLength);
			ramManager.unreserve(decompressedLength);

			// Discard the map-output
			try {
				input.discard();
			} catch (IOException ignored) {
				LOG.info("Failed to discard map-output from " + taskattemptid, 
						ignored);
			}
			input = null;

			// Close the streams
			IOUtils.cleanup(LOG, ins);

			// Re-throw
			throw ioe;
		} 

		// Close the in-memory file
		ramManager.closeInMemoryFile(decompressedLength);

		// Sanity check
		if (bytesRead != decompressedLength) {
			// Inform the ram-manager
			ramManager.unreserve(decompressedLength);

			// Discard the map-output
			try {
				input.discard();
			} catch (IOException ignored) {
				// IGNORED because we are cleaning up
				LOG.info("Failed to discard map-output from " + taskattemptid, 
						ignored);
			}
			input = null;

			throw new IOException("Incomplete map output received for " +
					taskattemptid + " (" + 
					bytesRead + " instead of " + 
					decompressedLength + ")"
			);
		}

		if (input != null) {
			synchronized (inputFilesInMemory) {
				inputFilesInMemory.add(input);
			}
      LOG.info("Returning true from shuffleInMemory()");
			return true;
		}
    LOG.info("Returning false from shuffleInMemory()");
		return false;
	} // shuffleInMemory

	private void configureClasspath(JobConf conf)
	throws IOException {

		// get the task and the current classloader which will become the parent
		ClassLoader parent = conf.getClassLoader();   

		// get the work directory which holds the elements we are dynamically
		// adding to the classpath
		File workDir = new File(task.getJobFile()).getParentFile();
		ArrayList<URL> urllist = new ArrayList<URL>();

		// add the jars and directories to the classpath
		String jar = conf.getJar();
		if (jar != null) {      
			File jobCacheDir = new File(new Path(jar).getParent().toString());

			File[] libs = new File(jobCacheDir, "lib").listFiles();
			if (libs != null) {
				for (int i = 0; i < libs.length; i++) {
					urllist.add(libs[i].toURL());
				}
			}
			urllist.add(new File(jobCacheDir, "classes").toURL());
			urllist.add(jobCacheDir.toURL());

		}
		urllist.add(workDir.toURL());

		// create a new classloader with the old classloader as its parent
		// then set that classloader as the one used by the current jobconf
		URL[] urls = urllist.toArray(new URL[urllist.size()]);
		URLClassLoader loader = new URLClassLoader(urls, parent);
		conf.setClassLoader(loader);
	}

	private long createInMemorySegments(List<Segment<K, V>> inMemorySegments, long leaveBytes)
	throws IOException {
		long totalSize = 0L;
		synchronized (inputFilesInMemory) {
			// fullSize could come from the RamManager, but files can be
			// closed but not yet present in mapOutputsFilesInMemory
			long fullSize = 0L;
			for (JInput in : inputFilesInMemory) {
				fullSize += in.data.length;
			}
			while(fullSize > leaveBytes) {
				JInput in = inputFilesInMemory.remove(0);
				totalSize += in.data.length;
				fullSize -= in.data.length;
				Reader<K, V> reader = 
					new InMemoryReader<K, V>((RamManager)ramManager, in.taskattemptid,
							in.data, 0, in.data.length);
				Segment<K, V> segment = new Segment<K, V>(reader, true);
				inMemorySegments.add(segment);
				in.discard();
			}
		}
		return totalSize;
	}

	/**
	 * Create a RawKeyValueIterator from copied map outputs. All copying
	 * threads have exited, so all of the map outputs are available either in
	 * memory or on disk. We also know that no merges are in progress, so
	 * synchronization is more lax, here.
	 *
	 * The iterator returned must satisfy the following constraints:
	 *   1. Fewer than io.sort.factor files may be sources
	 *   2. No more than maxInMemReduce bytes of map outputs may be resident
	 *      in memory when the reduce begins
	 *
	 * If we must perform an intermediate merge to satisfy (1), then we can
	 * keep the excluded outputs from (2) in memory and include them in the
	 * first merge pass. If not, then said outputs must be written to disk
	 * first.
	 */
	@SuppressWarnings("unchecked")
	public RawKeyValueIterator 
	//createKVIterator(JobConf job, FileSystem fs, Reporter reporter) throws IOException {
	createKVIterator(JobConf job, Reporter reporter) throws IOException {
		
		//final Path tmpDir = new Path(task.getTaskID().toString());
		TaskAttemptID taskattemptid = null;
		
    /*
		if (!open && inputFilesInMemory.size() > 0) {
			flush(maxInMemMerge);
		} else 
    */
    if (open) {
			if (inputFilesInMemory.size() > 0) {
				taskattemptid = inputFilesInMemory.get(0).taskattemptid;
			} 
      /*else if (inputFilesOnDisk.size() > 0) {
				taskattemptid = inputFilesOnDisk.first().taskattemptid;
			}
      */
		}
		
		
		// Get all segments on disk
		//List<Segment<K,V>> diskSegments = new ArrayList<Segment<K,V>>();
		long onDiskBytes = 0;
    /*
		synchronized (inputFilesOnDisk) {
			Path[] onDisk = getFiles(fs);
			for (Path file : onDisk) {
				onDiskBytes += fs.getFileStatus(file).getLen();
				diskSegments.add(new Segment<K, V>(job, fs, file, codec, false));
			}
			inputFilesOnDisk.clear();
		}
		LOG.info("Merging " + diskSegments.size() + " files, " +
				onDiskBytes + " bytes from disk");
		Collections.sort(diskSegments, new Comparator<Segment<K,V>>() {
			public int compare(Segment<K, V> o1, Segment<K, V> o2) {
				if (o1.getLength() == o2.getLength()) {
					return 0;
				}
				return o1.getLength() < o2.getLength() ? -1 : 1;
			}
		});
    */

		// Get all in-memory segments
		List<Segment<K,V>> finalSegments = new ArrayList<Segment<K,V>>();
		long inMemBytes = createInMemorySegments(finalSegments, 0);
		
		
		RawKeyValueIterator riter = null;
		
		// Create and return a merger
/*
		if (0 != onDiskBytes) {
			// build final list of segments from merged backed by disk + in-mem
			final int numInMemSegments = finalSegments.size();
			finalSegments.addAll(diskSegments);

			LOG.info("Merging " + finalSegments.size() + " segments, " +
					(inMemBytes + onDiskBytes) + " bytes from memory");
			riter = Merger.merge(
					job, fs, keyClass, valClass, codec, finalSegments,
					ioSortFactor, numInMemSegments, tmpDir, comparator,
					reporter, false, null, null);
		} else {
*/
 	  // All that remains is in-memory segments // TODO -jbuck make sure that this works with no file system passed in
		LOG.info("Merging " + finalSegments.size() + " segments, " +
				inMemBytes + " bytes from memory");
		riter = Merger.merge(job, null, keyClass, valClass,
		 	  finalSegments, finalSegments.size(), null,
				comparator, reporter, null, null);
		//}
		
		if (open && taskattemptid != null) {
			//return new RawKVIteratorWriter(riter, taskattemptid, inMemBytes + onDiskBytes);
			return new RawKVIteratorWriter(riter, taskattemptid, inMemBytes);
		}
		return riter;
	}
	
	/**
	 * An iterator that concurrent writes to a new spill file while client
	 * is reading. This is used for online aggregation. Basically, the task
	 * has called for an iterator while the input buffer is open. We take all
	 * current in-memory and on-disk runs and create a sort-merge iterator. That
	 * sort-merge iterator is passed to this wrapper class, which simulates the
	 * iterator while concurrent writing to a new spill file. When close is called
	 * the new spill file is registered as a new run, using the given taskid, and
	 * will be available during the next iterator call.
	 *
	 */
	class RawKVIteratorWriter implements RawKeyValueIterator {
		private RawKeyValueIterator riter;
		
		private IFile.Writer<K, V> writer;
		
		//private Path outputPath;
    ByteArrayOutputStream baOut;
    DataOutputStream out;
		
		//private TaskID taskid;
		private TaskAttemptID taskattemptid;
		
		//public RawKVIteratorWriter(RawKeyValueIterator riter, TaskID taskid, long bytes) throws IOException {
		public RawKVIteratorWriter(RawKeyValueIterator riter, TaskAttemptID taskattemptid, 
                               long bytes) throws IOException {
			this.riter = riter;
			this.taskattemptid = taskattemptid;
			//this.outputPath = outputHandle.getInputFileForWrite(task.getTaskID(), taskattemptid.getTaskID(), 
      //                                                    spills++, bytes);
      this.baOut =  new ByteArrayOutputStream((int)bytes);
      this.out = new DataOutputStream(baOut);

			this.writer = new IFile.Writer(conf, out, 
					                       keyClass, valClass, codec, null);;
		}

		@Override
		public void close() throws IOException {
			writer.close();
			
      // TODO --jbuck here
			// Register the output of the merge iterator
      byte[] outputBytes = this.baOut.toByteArray();
      if (outputBytes.length > 0) { 
			  synchronized (inputFilesInMemory) {
		      JInput input = new JInput(this.taskattemptid, outputBytes, outputBytes.length);
				  inputFilesInMemory.add(input);
			  }
      } else { 
        LOG.info("I would have added a JInput, but there was zero data");
      }
		}

		@Override
		public DataInputBuffer getKey() throws IOException {
			return riter.getKey();
		}

		@Override
		public Progress getProgress() {
			return riter.getProgress();
		}

		@Override
		public DataInputBuffer getValue() throws IOException {
			return riter.getValue();
		}

    public int getNumRecordsRepresented() throws IOException { 
      return riter.getNumRecordsRepresented();
    }

		@Override
		public boolean next() throws IOException {
			if (riter.next()) {
				writer.append(riter.getKey(), riter.getValue(), riter.getNumRecordsRepresented());
				return true;
			}
			return false;
		}
		
	}

	/*
	class RawKVIteratorReader extends IFile.Reader<K,V> {
		private final RawKeyValueIterator kvIter;

		public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
		throws IOException {
			super(null, null, size, null, null);
			this.kvIter = kvIter;
		}

		public boolean next(DataInputBuffer key, DataInputBuffer value)
		throws IOException {
			if (kvIter.next()) {
				final DataInputBuffer kb = kvIter.getKey();
				final DataInputBuffer vb = kvIter.getValue();
				final int kp = kb.getPosition();
				final int klen = kb.getLength() - kp;
				key.reset(kb.getData(), kp, klen);
				final int vp = vb.getPosition();
				final int vlen = vb.getLength() - vp;
				value.reset(vb.getData(), vp, vlen);
				bytesRead += klen + vlen;
				return true;
			}
			return false;
		}

		public long getPosition() throws IOException {
			return bytesRead;
		}

		public void close() throws IOException {
			kvIter.close();
		}
	}
	*/

  /*
	private void addInputFilesOnDisk(JInput input) throws IOException {
		synchronized (inputFilesOnDisk) {
			inputFilesOnDisk.add(input);
			inputFilesOnDisk.notifyAll();
			LOG.info("Total input files on disk " + inputFilesOnDisk.size());
		}
	}
  */

	private class InMemFSMergeThread extends Thread {

		public InMemFSMergeThread() {
			setName("Thread for merging in memory files");
			setPriority(Thread.MAX_PRIORITY);
			setDaemon(true);
		}

		public void run() {
			LOG.info(task.getTaskID() + " Thread started: " + getName());
			try {
				boolean exit = false;
				do {
					exit = ramManager.waitForDataToMerge();
					if (!exit) {
						flush();
					}
				} while (!exit);
			} catch (Exception e) {
				LOG.warn(task.getTaskID() +
						" Merge of the inmemory files threw an exception: "
						+ StringUtils.stringifyException(e));
				mergeThrowable = e;
			} catch (Throwable t) {
				String msg = task.getTaskID() + " : Failed to merge in memory" 
				+ StringUtils.stringifyException(t);
				LOG.error(msg);
				mergeThrowable = t;
			}
		}
	}
}

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.mapred.buffer.OutputInMemoryBuffer;
import org.apache.hadoop.mapred.buffer.OutputInMemoryBuffer.Header;
import org.apache.hadoop.mapred.buffer.impl.ValuesIterator;
import org.apache.hadoop.util.Progress;

public interface InputCollector<K extends Object, V extends Object> {
	
	//public boolean read(DataInputStream istream, OutputFile.Header header)
	public boolean read(DataInputStream istream, OutputInMemoryBuffer.Header header)
	throws IOException;
	
	public ValuesIterator<K, V> valuesIterator() throws IOException;
	
	public void flush() throws IOException;
	
	public void free();

	public void close();

}

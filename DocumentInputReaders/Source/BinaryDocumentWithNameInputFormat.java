package com.microsoft.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Reads complete documents in Binary format.
 */
public class BinaryDocumentWithNameInputFormat
	extends FileInputFormat<Text, BytesWritable> {

	public BinaryDocumentWithNameInputFormat() {
		super();
	}
	
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter) throws IOException {
		
		return new BinaryDocumentWithNameRecordReader((FileSplit) split, job);		
	}
	
    /**
    * BinaryDocumentWithNameRecordReader class to read through a given binary document
    * Outputs the filename along with the complete document
    */	
	public class BinaryDocumentWithNameRecordReader
		implements RecordReader<Text, BytesWritable> {
		
		private final FileSplit fileSplit;
		private final Configuration conf;
		private boolean processed = false;
		
		public BinaryDocumentWithNameRecordReader(FileSplit fileSplit, Configuration conf)
			throws IOException {
			this.fileSplit = fileSplit;
			this.conf = conf;
		}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public BytesWritable createValue() {
			return new BytesWritable();
		}
		  
		@Override
		public long getPos() throws IOException {
			return this.processed ? this.fileSplit.getLength() : 0;
		}
		
		@Override
		public float getProgress() throws IOException {
			return this.processed ? 1.0f : 0.0f;
		}
		
		@Override
		public boolean next(Text key, BytesWritable value) throws IOException {
			if (!this.processed) {
				byte[] contents = new byte[(int) this.fileSplit.getLength()];
				Path file = this.fileSplit.getPath();
				FileSystem fs = file.getFileSystem(this.conf);
				FSDataInputStream in = null;
				
				try {
					in = fs.open(file);
					in.readFully(contents, 0, contents.length);
					
					key.set(file.getName());
					value.set(contents, 0, contents.length);
				}
				finally {
					in.close();
				}
				
				this.processed = true;
				return true;
			}
			else {
				return false;
			}
		}

		@Override
		public void close() throws IOException {
		}
	}	
}

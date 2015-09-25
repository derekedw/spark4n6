package com.edwardsit.spark4n6;

import edu.nps.jlibewf.EWFFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
* Created by Derek on 9/22/2014.
*/
public class EWFRecordReader extends SequenceFileRecordReader<LongWritable, BytesWritable> {
    private static Logger log =Logger.getLogger(EWFRecordReader.class);
    private EWFFileReader stream = null;

    public EWFRecordReader() { }

    long start = 0L;
    long end = 0L;
    boolean notReadYet = true;
    boolean atEOF = false;
    BytesWritable currentValue = new BytesWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();
        final Path file = fileSplit.getPath();
        final FileSystem fs = file.getFileSystem(conf);
        start = fileSplit.getStart();
        end  = start + fileSplit.getLength();
        stream = new EWFFileReader(file.getFileSystem(context.getConfiguration()), file);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (atEOF)
            return false;
        if (notReadYet)
            notReadYet = false;
        long bytesToRead = (end - start);
        log.debug("stream.readImageBytes(" + start + ", (int) " + bytesToRead +");");
        byte[] buf = stream.readImageBytes(start, (int) bytesToRead);
        currentValue.set(buf,0,buf.length);
        atEOF = ((start + buf.length) >= end);
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return new LongWritable(start);
    }

    @Override
    public BytesWritable getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        return 0.0f;
    }

    @Override
    public void close() throws IOException {
        if(stream != null)
            stream.close();
    }
}

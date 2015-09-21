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
    private int len64KiB = 64 * 1024;
    private EWFFileReader stream = null;

    public EWFRecordReader() { }

    // FSDataInputStream stream = null;
    long start = 0L;
    long currentStart = 0L;
    long currentEnd = 0L;
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
        else if (notReadYet) {
            currentStart = start;
            notReadYet = false;
        } else {
            currentStart = currentEnd;
        }
        long bytesToRead = ((end - currentStart) > len64KiB) ? len64KiB : end - currentStart;
        log.debug("stream.readImageBytes(" + currentStart + ", (int) " + bytesToRead +");");
        byte[] buf = stream.readImageBytes(currentStart, (int) bytesToRead);
        currentValue.set(buf,0,buf.length);
        currentEnd = currentStart + buf.length;
        atEOF = (currentEnd >= end);
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return new LongWritable(currentStart);
    }

    @Override
    public BytesWritable getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        if (start == end)
            return 0.0f;
        else
            return (float) (currentEnd - start) / (end - start);
    }

    @Override
    public void close() throws IOException {
        if(stream != null)
            stream.close();
    }
}

package com.edwardsit.spark4n6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
* Created by Derek on 9/22/2014.
*/
public class EWFRecordReader extends RecordReader<LongWritable, BytesWritable> {
    private int len64KiB = 64 * 1024;

    public EWFRecordReader() { }

    FSDataInputStream stream = null;
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
        stream = fs.open(file);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (atEOF || currentEnd >= end)
            return false;
        else if (notReadYet) {
            currentStart = start;
            stream.seek(currentStart);
            notReadYet = false;
        } else {
            currentStart = currentEnd;
        }
        long bytesToRead = ((end - currentStart) > len64KiB) ? len64KiB : end - currentStart;
        byte[] buf = new byte[(int) bytesToRead];
        int bytesRead = stream.read(currentStart,buf,0,(int) bytesToRead);
        currentValue.set(buf,0,bytesRead);
        currentEnd = currentStart + bytesRead;
        return currentEnd < end;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(currentStart);
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
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

/*
 * Copyright 2015-2016 Derek Edwards
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.edwardsit.spark4n6;

import edu.nps.jlibewf.EWFFileReader;
import edu.nps.jlibewf.EWFSegmentFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
* Created by Derek on 9/22/2014.
*/
public class EWFRecordReader extends SequenceFileRecordReader<BytesWritable, BytesWritable> {
    private static Logger log = Logger.getLogger(EWFRecordReader.class);
    private static long nChunksPerRecord = -1L;
    private long chunkSize = 0L;
    private EWFFileReader stream = null;
    public EWFRecordReader() { }
    long start = 0L;
    long end = 0L;
    long currentStart = 0L;
    long currentEnd = 0L;
    boolean notReadYet = true;
    boolean atEOF = false;
    BytesWritable currentKey = new BytesWritable();
    BytesWritable currentValue = new BytesWritable();
    FileSystem fs = null;
    Path file = null;

    protected Path getFirstFile() {
        int length = file.getName().length();
        Path parent = file.getParent();
	    return new Path(parent,file.getName().substring(0,length-4) + ".E01");
    }
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();
        file = fileSplit.getPath();
        start = fileSplit.getStart();
        end  = start + fileSplit.getLength();
        fs = file.getFileSystem(conf);
        stream = new EWFFileReader(fs,getFirstFile());
        chunkSize = new EWFSegmentFileReader(fs).DEFAULT_CHUNK_SIZE;
        log.setLevel(Level.DEBUG);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (atEOF || currentEnd >= end)
            return false;
        else if (notReadYet) {
            currentStart = start;
            notReadYet = false;
        } else {
            currentStart = currentEnd;
        }
        long bytesToRead = ((end - currentStart) > (getChunksPerRecord() * chunkSize)) ? (getChunksPerRecord() * chunkSize) : (end - currentStart);
        byte[] valBuf = stream.readImageBytes(currentStart, (int) bytesToRead);
        log.debug("stream.readImageBytes(" + currentStart + ", (int) " + bytesToRead + ") = " + valBuf.length + ";");
        String filename = getFirstFile().toUri().toASCIIString();
        int len = filename.getBytes().length;
        ByteBuffer keyBuf = ByteBuffer.allocate(Long.SIZE/8 + Integer.SIZE/8 + len);
        keyBuf.putLong(currentStart);
        keyBuf.putInt(len);
        keyBuf.put(filename.getBytes());
        keyBuf.flip();
        currentKey.set(keyBuf.array(),0,Long.SIZE/8 + Integer.SIZE/8 + len);
        currentValue.set(valBuf, 0, valBuf.length);
        currentEnd = currentStart + valBuf.length;
        return currentEnd <= end;
    }

    @Override
    public BytesWritable getCurrentKey() {
        return new BytesWritable(currentKey.copyBytes());
    }

    @Override
    public BytesWritable getCurrentValue() {
        return new BytesWritable(currentValue.copyBytes());
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
    protected long getChunksPerRecord() throws IOException {
        if (nChunksPerRecord == -1L) {
            long hdfsBlockSize = fs.getFileStatus(file).getBlockSize();
            // nChunksPerRecord = (hdfsBlockSize/chunkSize/8) - 1L;
            nChunksPerRecord = 2L; // 64 KiB, the default block size for HBase
        }
        return nChunksPerRecord;
    }
}

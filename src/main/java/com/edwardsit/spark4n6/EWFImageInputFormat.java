package com.edwardsit.spark4n6;

import edu.nps.jlibewf.EWFFileReader;
import edu.nps.jlibewf.EWFSection;
import edu.nps.jlibewf.EWFSegmentFileReader;
import org.apache.hadoop.fs.*;
import org.apache.log4j.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
* Created by Derek on 9/22/2014.
*/
public class EWFImageInputFormat extends FileInputFormat<LongWritable,BytesWritable> {
    private static Logger log = Logger.getLogger(EWFImageInputFormat.class);

    public EWFImageInputFormat() { }

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new EWFRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        log.setLevel(Level.DEBUG);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        BlockLocation[] blkLocations = null;
        Path path = null;
        FileSystem fs = null;
        EWFFileReader ewf = null;
        for (FileStatus file: files) {
            path = file.getPath();
            fs = path.getFileSystem(job.getConfiguration());
            if (path.getName().endsWith(".E01")) {
                if (file instanceof LocatedFileStatus) {
                    blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                } else {
                    blkLocations = fs.getFileBlockLocations(path, 0L, 512);
                }
                ewf = new EWFFileReader(fs, path);
                log.debug("splits.add(makeSplit(" + path + ", 0L, " + ewf.getImageSize() + ", " + listHosts(blkLocations) + ");");
                splits.add(makeSplit(path, 0L, ewf.getImageSize(), blkLocations[0].getHosts(), blkLocations[0].getCachedHosts()));
            }
        }
        return splits;
    }
    protected String listHosts(BlockLocation[] blkLocations) throws IOException {
        StringBuffer hosts = new StringBuffer();
        hosts.append("[");
        for (String host : blkLocations[0].getHosts()) { hosts.append(host).append(" "); }
        hosts.append("],[");
        for (String host : blkLocations[0].getCachedHosts()) { hosts.append(host).append(" "); }
        hosts.append("]");
        return hosts.toString();
    }
    protected int findBlockIndex(BlockLocation[] blkLocations,EWFSection.SectionPrefix sp) throws IOException {
        int blkIndex = 0;
        blkIndex = this.getBlockIndex(blkLocations,sp.fileOffset);
        return blkIndex;
    }
}

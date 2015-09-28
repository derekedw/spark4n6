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
    private EWFFileReader ewf = null;
    private Path filename = null;
    private FileSystem fs = null;

    public EWFImageInputFormat() { }

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new EWFRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        this.filename = filename;
        try {
            this.fs = this.filename.getFileSystem(context.getConfiguration());
            ewf = new EWFFileReader(fs, filename);
            return false;
        } catch (IOException ioe) {
            return false;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        log.setLevel(Level.DEBUG);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        FileStatus status;
        BlockLocation[] blkLocations = { new BlockLocation() };
        if (ewf == null) {
            return super.getSplits(job);
        } else {
            status = fs.getFileStatus(filename);
            if (status instanceof LocatedFileStatus) {
                blkLocations = ((LocatedFileStatus) status).getBlockLocations();
            } else {
                blkLocations = fs.getFileBlockLocations(filename, 0L, 512);
            }
            log.debug("splits.add(makeSplit(" + filename + ", 0L, " + ewf.getImageSize() + ", " + listHosts(blkLocations) + ");");
            splits.add(makeSplit(filename, 0L, ewf.getImageSize(), blkLocations[0].getHosts(), blkLocations[0].getCachedHosts()));
        }
            /* ArrayList<EWFSection.SectionPrefix> sections = ewf.getSectionPrefixArray();
            Iterator<EWFSection.SectionPrefix> it = sections.iterator();
            EWFSection.SectionPrefix sp, priorSP = null;
            long priorStart = 0L;
            long chunkCount = 0L;
            int blkIndex = 0;
            String hosts = "";
            while (it.hasNext()) {
                sp = it.next();
                if (sp.sectionType.equals(EWFSection.SectionType.TABLE_TYPE)) {
                    chunkCount += sp.chunkCount;
                    while (chunkCount >= getChunksPerSplit(job)) {
                        blkIndex = findBlockIndex(blkLocations,priorSP);
                        hosts = listHosts(blkLocations, priorSP, blkIndex);
                        log.debug("splits.add(makeSplit(" + filename + "," + (priorStart * chunkSize) + "," +
                                (getChunksPerSplit(job) * chunkSize) + "," + hosts + "));");
                        splits.add(makeSplit(filename, (priorStart * chunkSize), (getChunksPerSplit(job) * chunkSize),
                                blkLocations[blkIndex].getHosts(),blkLocations[blkIndex].getCachedHosts()));
                        priorStart += getChunksPerSplit(job);
                        chunkCount -= getChunksPerSplit(job);
                    }
                }

                priorSP = sp;
            }
            log.debug("splits.add(makeSplit(" + filename + "," + (priorStart * chunkSize) + "," +
                    (chunkCount * chunkSize)+ "," + hosts + "));");
            splits.add(makeSplit(filename, (priorStart * chunkSize),(chunkCount * chunkSize),
                    blkLocations[blkIndex].getHosts(),blkLocations[blkIndex].getCachedHosts()));
        }*/
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

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
    private long chunkSize = new EWFSegmentFileReader(fs).DEFAULT_CHUNK_SIZE;
    private static long nChunksPerSplit = -1L;

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
            return true;
        } catch (IOException ioe) {
            return false;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
	    log.setLevel(Level.DEBUG);
        if (ewf == null) {
            return super.getSplits(job);
        } else {
            log.debug("imageSize = " + ewf.getImageSize() / chunkSize +", chunksPerSplit = " + getChunksPerSplit(job));
            ArrayList<EWFSection.SectionPrefix> sections = ewf.getSectionPrefixArray();
            Iterator<EWFSection.SectionPrefix> it = sections.iterator();
            BlockLocation[] blkLocations = { new BlockLocation() };
            EWFSection.SectionPrefix sp, priorSP = null;
            long priorStart = 0L;
            long chunkCount = 0L;
            FileStatus status;
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
                status = fs.getFileStatus(sp.file);
                if (status instanceof LocatedFileStatus) {
                    blkLocations = ((LocatedFileStatus) status).getBlockLocations();
                } else {
                    blkLocations = fs.getFileBlockLocations(sp.file, sp.fileOffset, sp.sectionSize);
                }
                priorSP = sp;
            }
            log.debug("splits.add(makeSplit(" + filename + "," + (priorStart * chunkSize) + "," +
                    (chunkCount * chunkSize)+ "," + hosts + "));");
            splits.add(makeSplit(filename, (priorStart * chunkSize),(chunkCount * chunkSize),
                    blkLocations[blkIndex].getHosts(),blkLocations[blkIndex].getCachedHosts()));
        }
        return splits;
    }
    protected String listHosts(BlockLocation[] blkLocations,EWFSection.SectionPrefix sp, int blkIndex) throws IOException {
        StringBuffer hosts = new StringBuffer();
        hosts.append("[");
        for (String host : blkLocations[blkIndex].getHosts()) { hosts.append(host).append(" "); }
        hosts.append("],[");
        for (String host : blkLocations[blkIndex].getCachedHosts()) { hosts.append(host).append(" "); }
        hosts.append("]");
        log.debug(sp.file.getName() + "," + sp.fileOffset + "," + sp.sectionSize + ", is on " + hosts);
        return hosts.toString();
    }
    protected int findBlockIndex(BlockLocation[] blkLocations,EWFSection.SectionPrefix sp) throws IOException {
        int blkIndex = 0;
        blkIndex = this.getBlockIndex(blkLocations,sp.fileOffset);
        return blkIndex;
    }
    protected long getChunksPerSplit(JobContext job) throws IOException {
        long maxSize = 0L;
        long blockSize = 0L;
        long splitSize = 0L;
        if (nChunksPerSplit == -1L) {
            maxSize = getMaxSplitSize(job);
            blockSize = fs.getFileStatus(filename).getBlockSize();
            splitSize = computeSplitSize(blockSize, chunkSize, maxSize);
            nChunksPerSplit = (splitSize/chunkSize) - 1L;
        }
        return nChunksPerSplit;
    }
}

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
public class EWFImageInputFormat extends FileInputFormat<BytesWritable,BytesWritable> {
    private static Logger log = Logger.getLogger(EWFImageInputFormat.class);
    private long chunkSize = 0L;

    public EWFImageInputFormat() { }

    @Override
    public RecordReader<BytesWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new EWFRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
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
        ArrayList<EWFSection.SectionPrefix> sections = null;
        Iterator<EWFSection.SectionPrefix> it = null;
        EWFSection.SectionPrefix sp = null;
        Path priorFile = null;
        long priorOffset = 0L;
        FileStatus priorFileStatus = null;
        chunkSize = new EWFSegmentFileReader(fs).DEFAULT_CHUNK_SIZE;
        long priorStart = 0L;
        int blkIndex = 0;
        for (FileStatus file: files) {
            path = file.getPath();
            fs = path.getFileSystem(job.getConfiguration());
            if (path.getName().endsWith(".E01")) {

                ewf = new EWFFileReader(fs, path);
                sections = ewf.getSectionPrefixArray();
                it = sections.iterator();
                while(it.hasNext()) {
                    sp = it.next();
                    if (sp.sectionType.equals(EWFSection.SectionType.TABLE_TYPE)) {
                        priorFileStatus = fs.getFileStatus(priorFile);
                        for (long i = sp.chunkCount; i > 0L; i = i - getChunksPerSplit(priorFileStatus)) {
                            if (priorFileStatus instanceof LocatedFileStatus) {
                                blkLocations = ((LocatedFileStatus) priorFileStatus).getBlockLocations();
                            } else {
                                blkLocations = fs.getFileBlockLocations(priorFileStatus, priorOffset, (getChunksPerSplit(priorFileStatus) * chunkSize));
                            }
                            blkIndex = getBlockIndex(blkLocations, priorOffset);
                            if (i > getChunksPerSplit(priorFileStatus)) {
                                log.debug("splits.add(makeSplit(" + priorFile + ", " + (priorStart * chunkSize) + ", " + (getChunksPerSplit(priorFileStatus) * chunkSize) + ", " + listHosts(blkLocations, blkIndex) + ");");
                                splits.add(makeSplit(priorFile, (priorStart * chunkSize), (getChunksPerSplit(priorFileStatus) * chunkSize), blkLocations[blkIndex].getHosts()));
                                priorStart += getChunksPerSplit(priorFileStatus);
                            } else {
                                log.debug("splits.add(makeSplit(" + priorFile + ", " + (priorStart * chunkSize) + ", " + (i * chunkSize) + ", " + listHosts(blkLocations, blkIndex) + ");");
                                splits.add(makeSplit(priorFile, (priorStart * chunkSize), (i * chunkSize), blkLocations[blkIndex].getHosts()));
                                priorStart += i;
                            }
                        }
                    }
                    priorFile = sp.file;
                    priorOffset = sp.fileOffset;
                }
            }
        }
        return splits;
    }
    protected String listHosts(BlockLocation[] blkLocations,int blkIndex) throws IOException {
        StringBuffer hosts = new StringBuffer();
        hosts.append("[");
        for (String host : blkLocations[blkIndex].getHosts()) { hosts.append(host).append(" "); }
        hosts.append("]");
        return hosts.toString();
    }
    protected long getChunksPerSplit(FileStatus file) {
        return file.getBlockSize() / chunkSize;
    }
}

package com.edwardsit.spark4n6;

import edu.nps.jlibewf.EWFFileReader;
import edu.nps.jlibewf.EWFSection;
import org.apache.log4j.*;
import org.apache.hadoop.fs.Path;
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

    public EWFImageInputFormat() { }

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new EWFRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        this.filename = filename;
        try {
            ewf = new EWFFileReader(filename.getFileSystem(context.getConfiguration()), filename);
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
            long imageSize = ewf.getImageSize();
	    	log.debug("size = " + imageSize);
            ArrayList<EWFSection.SectionPrefix> sections = ewf.getSectionPrefixArray();
            Iterator<EWFSection.SectionPrefix> it = sections.iterator();
            EWFSection.SectionPrefix sp;
            long numSplits = job.getConfiguration().getInt(job.NUM_MAPS, -1);
            // For testing long numSplits = 10;
            assert numSplits >= 0 : "Couldn't find " + job.NUM_MAPS;
            long priorStart = 0L;
            long chunkCount = 0L;
            Path priorFile = null;
            while (it.hasNext()) {
                sp = it.next();
                if (sp.sectionType.equals(EWFSection.SectionType.TABLE_TYPE)) {
                    chunkCount += sp.chunkCount;
                    if (    (priorFile != null)
                            && !sp.file.equals(priorFile)
                            && isItLargeEnoughForASplit(chunkCount, numSplits, imageSize))
                    {
                        log.debug("splits.add(new FileSplit(" + filename + "," + (priorStart * 64L * 512L) + "," + (chunkCount * 64L * 512L) + ", null));");
                        splits.add(new FileSplit(filename, (priorStart * 64L * 512L), (chunkCount * 64L * 512L), null));
                        priorStart += chunkCount;
                        chunkCount = 0L;
                    }
                    priorFile = sp.file;
                }
            }
            log.debug("splits.add(new FileSplit(" + filename + "," + (priorStart * 64L * 512L) + "," + (chunkCount * 64L * 512L) + ", null));");
            splits.add(new FileSplit(filename,(priorStart * 64L * 512L),(chunkCount * 64L * 512L), null));
        }
        return splits;
    }
    boolean isItLargeEnoughForASplit(long chunkCount, long numSplits, long imageSize) {
        return (((float) ((64.0 * 512.0 * numSplits * chunkCount) / imageSize)) >= 1.0);
    }
}

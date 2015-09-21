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
import org.apache.log4j.spi.LoggingEvent;

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
            long size = ewf.getImageSize();
		log.debug("size = " + size);
            ArrayList<EWFSection.SectionPrefix> sections = ewf.getSectionPrefixArray();
            Iterator<EWFSection.SectionPrefix> it = sections.iterator();
            EWFSection.SectionPrefix sp;
            long numSplits = job.getMaxMapAttempts();
            long priorStart = 0L;
            long priorEnd = 0L;
            Path priorFile = null;
            while (it.hasNext()) {
                sp = it.next();
                if (sp.sectionType.equals(EWFSection.SectionType.TABLE2_TYPE)) {
                    priorEnd = sp.chunkIndex;
                }
            }
            log.debug("splits.add(new FileSplit(" + filename + "," + priorStart + "," + ((priorEnd - priorStart) * 64 * 512) + ", null));");
            splits.add(new FileSplit(filename,priorStart,((priorEnd - priorStart) * 64 * 512), null));
        }
        return splits;
    }
}

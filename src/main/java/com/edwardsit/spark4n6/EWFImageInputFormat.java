package com.edwardsit.spark4n6;

import edu.nps.jlibewf.EWFFileReader;
import edu.nps.jlibewf.EWFSection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
        filename = filename;
        try {
            ewf = new EWFFileReader(filename.getFileSystem(context.getConfiguration()), filename);
            return true;
        } catch (IOException ioe) {
            return false;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
	log.setLevel(Level.DEBUG);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        if (ewf == null) {
            return super.getSplits(job);
        } else {
            long size = ewf.getImageSize();
            ArrayList<EWFSection.SectionPrefix> sections = ewf.getSectionPrefixArray();
            Iterator<EWFSection.SectionPrefix> it = sections.iterator();
            EWFSection.SectionPrefix sp;
            long numSplits = job.getMaxMapAttempts();
            long priorStart = 0L;
            long priorEnd = 0L;
            Path priorFile = null;
            while (it.hasNext()) {
                sp = it.next();
                if (!sp.file.equals(priorFile) && sp.sectionType.equals(EWFSection.SectionType.TABLE_TYPE)) {
                    if (priorFile != null) {
                        priorEnd = sp.chunkIndex;
                        log.debug(priorFile + "Split#" + (numSplits * priorEnd * 64 * 512 / size) + ", " + priorStart + " to " + priorEnd);
                    }
                    priorFile = sp.file;
                    priorStart = sp.chunkIndex;
                    splits.add(new FileSplit(filename,priorStart,priorEnd - priorStart, null));
                }
            }
            log.debug(priorFile + "Split#" + (numSplits * priorEnd * 64 * 512 / size) + ", " + priorStart + " to " + size / 64 / 512);
        }
        return splits;
    }
}

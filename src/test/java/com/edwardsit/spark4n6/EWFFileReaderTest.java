package com.edwardsit.spark4n6;

import edu.nps.jlibewf.EWFFileReader;
import edu.nps.jlibewf.EWFSection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Derek on 9/27/2014.
 */
public class EWFFileReaderTest extends Configured {
    private static Logger log = Logger.getLogger(EWFFileReaderTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ConsoleAppender app = new ConsoleAppender(new PatternLayout(), "System.err");
        Logger.getRootLogger().addAppender(app);
    }

    @Test
    public void testGetEWFSection() throws IOException {
        log.setLevel(Level.DEBUG);
        Logger.getLogger("com.edwardsit.spark4n6").addAppender(new RollingFileAppender(new PatternLayout(), "debug.log"));
        Configuration conf = new Configuration(false);
        // Path path = new Path("../macwd.E01");
        Path path = new Path("D:\\Users\\Derek\\Images\\500GB\\500GB-CDrive.E01");
        FileSystem fs = path.getFileSystem(conf);

        EWFFileReader reader = new EWFFileReader(fs,path);
        long size = reader.getImageSize();
        ArrayList<EWFSection.SectionPrefix> sections = reader.getSectionPrefixArray();
        Iterator<EWFSection.SectionPrefix> it = sections.iterator();
        EWFSection.SectionPrefix sp;
        long numSplits = 10L;
        long priorStart = 0L;
        long priorEnd = 0L;
        Path priorFile = null;
        log.debug(path.getName() + ": imageSize = " + size);
        log.debug("File\t\tChunkIndex\t\tSectionType\t\tChunkCount\t\tSectionSize");
        while (it.hasNext()) {
            sp = it.next();
            assertNotNull(sp);
            log.debug(sp.file+"\t\t"+sp.chunkIndex+"\t\t"+sp.sectionType+"\t\t"+sp.chunkCount+"\t\t"+sp.sectionSize);
            if (!sp.file.equals(priorFile) && sp.sectionType.equals(EWFSection.SectionType.TABLE_TYPE)) {
                if (priorFile != null) {
                    priorEnd = sp.chunkIndex;
                    // log.debug(priorFile + "Split#" + (numSplits * priorEnd * 64 * 512 / size) + ", " + priorStart + " to " + priorEnd);
                }
                priorFile = sp.file;
                priorStart = sp.chunkIndex;
            }
        }
        // log.debug(priorFile + " Split#" + (numSplits * priorEnd * 64 * 512 / size) + ", " + priorStart + " to " + size / 64 / 512);
    }
}

package net.gandalf.journal;

import net.gandalf.journal.common.JournalTestUtil;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Check which settings let the file size only moderately grow.
 *
 * @author ratzlow@gmail.com
 * @since 2014-01-16
 */
public class ChronicleConfigFileSizeTest {

    private Logger logger = Logger.getLogger( ChronicleConfigFileSizeTest.class);
    private String EXT_DATA = ".data";
    private String EXT_INDEX = ".index";

    @Test
    public void testMinimalSize() throws IOException {
        String name = JournalTestUtil.createLogFileNameFixed("testMinimalSize");
        logger.info("Create chronicle files for baseName = " + name );
        IndexedChronicle chronicle = new IndexedChronicle(name, ChronicleConfig.SMALL);

        logger.info("Before adding an entry ...");
        logFileSizes(name);

        logger.info("Opening a tailer ...");
        ExcerptTailer tailer = chronicle.createTailer();
        tailer.close();

        logFileSizes(name);
        chronicle.close();
    }

    private void logFileSizes(String name) {
        logger.info("Data file size = " + getFileSize( name, EXT_DATA ));
        logger.info("Index file size = " + getFileSize( name, EXT_INDEX ));
    }

    private long getFileSize( String fileName, String extension ) {
        File file = new File(fileName + extension);
        long length = file.length();
        return length;
    }
}

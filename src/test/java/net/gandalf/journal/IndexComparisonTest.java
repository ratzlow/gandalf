package net.gandalf.journal;

import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import org.apache.log4j.Logger;
import org.junit.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Verify sequence of writer is monoton rising. The sequences are not monoton rising since some buckets might be padded.
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-05
 */
@Ignore
public class IndexComparisonTest {

    private static final Logger LOGGER = Logger.getLogger(IndexComparisonTest.class);

    private String basePath = "C:/temp/testChronicle";
    private final Map<String, String> values = createValues();
    private IndexedChronicle chronicle;
    private ExcerptAppender appender;

    @Test
    public void testWriteBigChronicle() throws IOException {
        String path = basePath + "simpleStringWrite";
        IndexedChronicle indexedChronicle = new IndexedChronicle(path);
        ExcerptAppender excerptAppender = indexedChronicle.createAppender();

        String randomString1024Bytes = createTestString();
        int MIO = 1024 * 1024;
        int BIL = 1024 * MIO;

        long start = System.currentTimeMillis();
        int noEntries = 2 * MIO;
        for (int i=0; i < noEntries; i++ ) {
            excerptAppender.startExcerpt(1050);
            excerptAppender.writeUTF( randomString1024Bytes );
            excerptAppender.finish();
        }
        indexedChronicle.close();
        LOGGER.info("Finished writing " + noEntries + " entries with each size " + randomString1024Bytes.length() +
                " duration (ms) " + (System.currentTimeMillis() - start)) ;
    }

    private String createTestString() {
        StringBuilder sb = new StringBuilder();
        for ( int i=0; i < 29; i++ )
            sb.append( UUID.randomUUID().toString() );

        return sb.toString().substring(0, 1024);
    }

    @Test
    public void testWriteSingleMap() throws IOException {
        bootChronicle("singleMap");
        writeChronicle(appender, values, new WriterStrategy() {
            @Override
            public void write(ExcerptAppender appender, Map<String, String> values) {
                appender.writeMap(values);
            }
        });
    }

    @Test
    public void testWriteMapValues() throws IOException {
        bootChronicle("multiValues");
        writeChronicle(appender, values, new WriterStrategy() {
            @Override
            public void write(ExcerptAppender appender, Map<String, String> values) {
                for (String key : values.keySet()) {
                    appender.writeChars(key);
                    appender.writeChars(values.get(key));
                }
            }
        });
    }

    //
    // test life cycle
    //

    @BeforeClass
    public static void initStatic() {
        deleteFiles();
    }

    @After
    public void after() throws IOException {
        if (chronicle != null) chronicle.close();
        deleteFiles();
    }

    //
    // utility methods
    //

    interface WriterStrategy {
        void write( ExcerptAppender appender, Map<String, String> values );
    }

    private void writeChronicle(ExcerptAppender appender, Map<String, String> values, WriterStrategy strategy) {
        int capacity = 50;
        for ( int i=0; i < 1000000; i++ ) {
            appender.startExcerpt(capacity);
            long index = appender.index();
            Assert.assertEquals(i, index);
            strategy.write( appender, values );
            appender.finish();
        }
    }


    private void bootChronicle(String discriminator) throws IOException {
        String path = basePath + discriminator ;
        chronicle = new IndexedChronicle(path, ChronicleConfig.SMALL);
        appender = chronicle.createAppender();
    }


    private static void deleteFiles() {
        String[] fileNames = new File("C:/temp").list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("testChronicle");
            }
        });

        for (String fileName : fileNames) {
            new File("C:/temp", fileName).delete();
        }
    }

    private Map<String, String> createValues() {
        Map<java.lang.String, java.lang.String> attributes = new HashMap<java.lang.String, java.lang.String>();
        attributes.put("firstname", "frank");

        return attributes;
    }
}

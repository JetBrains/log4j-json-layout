/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.appenders;

import com.jayway.jsonassert.JsonAsserter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.NDC;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.StringWriter;
import java.net.InetAddress;

import static com.jayway.jsonassert.JsonAssert.with;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class JsonLayoutTest {

    @Rule
    public TestName testName = new TestName();

    private StringWriter consoleWriter;
    private JsonLayout consoleLayout;
    private Logger logger;

    @Before
    public void setUp() throws Exception {
        consoleWriter = new StringWriter();

        consoleLayout = new JsonLayout();
        consoleLayout.activateOptions();

        ConsoleAppender consoleAppender = spy(new ConsoleAppender());
        doNothing().when(consoleAppender).activateOptions();
        consoleAppender.setWriter(consoleWriter);
        consoleAppender.setLayout(consoleLayout);
        consoleAppender.activateOptions();

        logger = Logger.getRootLogger();
        logger.addAppender(consoleAppender);
        logger.setLevel(Level.INFO);
    }

    @Test
    public void testDefaultFields() throws Exception {
        NDC.push("ndc_1");
        NDC.push("ndc_2");
        NDC.push("ndc_3");

        MDC.put("mdc_key_1", "1");
        MDC.put("mdc_key_2", 2L);
        MDC.put("mdc_key_3", 3);
        MDC.put("mdc_key_4", 4.1);

        @SuppressWarnings("ThrowableInstanceNeverThrown")
        RuntimeException exception = new RuntimeException("Hello World Exception");

        logger.error("Hello World", exception);

        JsonAsserter asserter = with(consoleWriter.toString())
            .assertThat("$.exception.message", equalTo(exception.getMessage()))
            .assertThat("$.exception.class", equalTo(exception.getClass().getName()));
        for (StackTraceElement e : exception.getStackTrace()) {
            asserter
                .assertThat("$.exception.stacktrace", containsString(e.getClassName()))
                .assertThat("$.exception.stacktrace", containsString(e.getMethodName()));
        }
        asserter
            .assertThat("$.level", equalTo("ERROR"))
            .assertThat("$.location", nullValue())
            .assertThat("$.logger", equalTo(logger.getName()))
            .assertThat("$.mdc.mdc_key_1", equalTo("1"))
            .assertThat("$.mdc.mdc_key_2", equalTo("2"))
            .assertThat("$.mdc.mdc_key_3", equalTo("3"))
            .assertThat("$.mdc.mdc_key_4", equalTo("4.1"))
            .assertThat("$.message", equalTo("Hello World"))
            .assertThat("$.ndc", equalTo("ndc_1 ndc_2 ndc_3"))
            .assertThat("$.path", nullValue())
            .assertThat("$.host", equalTo(InetAddress.getLocalHost().getHostName()))
            .assertThat("$.tags", nullValue())
            .assertThat("$.thread", equalTo(Thread.currentThread().getName()))
            .assertThat("$.@timestamp", notNullValue())
            .assertThat("$.@version", equalTo("1"));
    }

    @Test
    public void testIncludeFields() throws Exception {
        consoleLayout.setRenamedFieldLabels("location:renamed_location,location.file:renamed_file");
        consoleLayout.setIncludedFields("location");
        consoleLayout.activateOptions();

        logger.info("Hello World");

        with(consoleWriter.toString())
            .assertThat("$.location", nullValue())
            .assertThat("$.renamed_location", notNullValue())
            .assertThat("$.renamed_location.class", equalTo(getClass().getName()))
            .assertThat("$.renamed_location.renamed_file", equalTo(getClass().getSimpleName() + ".java"))
            .assertThat("$.renamed_location.method", equalTo(testName.getMethodName()))
            .assertThat("$.renamed_location.line", notNullValue());
    }

    @Test
    public void testJSONIsValid() throws Exception {
        final StringBuilder message = new StringBuilder("Hello World: ");
        for(int c = Character.MIN_VALUE; c <= Character.MAX_VALUE; c++) {
            message.append((char)c);
        }

        consoleLayout.activateOptions();
        logger.info(message.toString());

        with(consoleWriter.toString())
                .assertThat("$.message", startsWith("Hello World: "));
    }

    @Test
    public void testExcludeFields() throws Exception {
        consoleLayout.setRenamedFieldLabels("ndc:renamed_ndc");
        consoleLayout.setExcludedFields("ndc,mdc,exception");
        consoleLayout.activateOptions();

        NDC.push("ndc_1");
        NDC.push("ndc_2");
        NDC.push("ndc_3");

        MDC.put("mdc_key_1", "mdc_val_1");
        MDC.put("mdc_key_2", "mdc_val_2");

        @SuppressWarnings("ThrowableInstanceNeverThrown")
        RuntimeException exception = new RuntimeException("Hello World Exception");

        logger.error("Hello World", exception);

        with(consoleWriter.toString())
            .assertThat("$.exception", nullValue())
            .assertThat("$.level", equalTo("ERROR"))
            .assertThat("$.logger", equalTo(logger.getName()))
            .assertThat("$.mdc", nullValue())
            .assertThat("$.message", equalTo("Hello World"))
            .assertThat("$.ndc", nullValue())
            .assertThat("$.renamed_ndc", nullValue())
            .assertThat("$.path", nullValue())
            .assertThat("$.host", equalTo(InetAddress.getLocalHost().getHostName()))
            .assertThat("$.tags", nullValue())
            .assertThat("$.thread", equalTo(Thread.currentThread().getName()))
            .assertThat("$.@timestamp", notNullValue())
            .assertThat("$.@version", equalTo("1"));
    }

    @Test
    public void testAddTags() throws Exception {
        consoleLayout.setTags("json,logstash");
        consoleLayout.activateOptions();

        logger.info("Hello World");

        with(consoleWriter.toString()).assertThat("$.tags", hasItems("json", "logstash"));
    }

    @Test
    public void testAddFields() throws Exception {
        consoleLayout.setFields("type:log4j,shipper:logstash");
        consoleLayout.activateOptions();

        logger.info("Hello World");

        with(consoleWriter.toString())
            .assertThat("$.type", equalTo("log4j"))
            .assertThat("$.shipper", equalTo("logstash"));
    }

    @Test
    public void testRenameFieldLabel() throws Exception {
        consoleLayout.setRenamedFieldLabels("level:renamed_level,tags:renamed_tags,@version:@renamed_version");
        consoleLayout.setTags("json");
        consoleLayout.activateOptions();

        logger.info("Hello World");

        with(consoleWriter.toString())
                .assertThat("$.level", nullValue())
                .assertThat("$.renamed_level", equalTo("INFO"))
                .assertThat("$.tags", nullValue())
                .assertThat("$.renamed_tags", hasItems("json"))
                .assertThat("$.@version", nullValue())
                .assertThat("$.@renamed_version", equalTo("1"));
    }

    @Test
    public void testRenameExceptionFieldLabel() throws Exception {
        consoleLayout.setRenamedFieldLabels("exception.message:renamed_message");
        consoleLayout.activateOptions();

        logger.info("Hello World", new RuntimeException("Test"));

        with(consoleWriter.toString())
                .assertThat("$.exception.message", nullValue())
                .assertThat("$.exception.renamed_message", equalTo("Test"));
    }

    @Test
    public void testRenameLocationFieldLabel() throws Exception {
        consoleLayout.setRenamedFieldLabels("location.method:renamed_method");
        consoleLayout.setIncludedFields("location");
        consoleLayout.activateOptions();

        logger.info("Hello World", new RuntimeException("Test"));

        with(consoleWriter.toString())
                .assertThat("$.location.method", nullValue())
                .assertThat("$.location.renamed_method", equalTo("testRenameLocationFieldLabel"));
    }

    @Test
    public void testSourcePath() throws Exception {
        logger.info("Hello World!");
        with(consoleWriter.toString()).assertThat("$.path", nullValue());

        // for the file appender there must be log file path in the json
        StringWriter fileWriter = new StringWriter();

        JsonLayout fileLayout = new JsonLayout();
        fileLayout.activateOptions();

        FileAppender fileAppender = spy(new FileAppender());
        doNothing().when(fileAppender).activateOptions();
        fileAppender.setWriter(fileWriter);
        fileAppender.setFile("/tmp/logger.log");
        fileAppender.setLayout(fileLayout);
        fileAppender.activateOptions();

        logger.addAppender(fileAppender);

        logger.info("Hello World!");
        with(fileWriter.toString())
            .assertThat("$.path", equalTo(new File(fileAppender.getFile()).getCanonicalPath()));
    }

    @Test
    public void testParentLoggerSourcePath() throws Exception {
        logger.info("Hello World!");
        with(consoleWriter.toString()).assertThat("$.path", nullValue());

        // for the file appender there must be log file path in the json
        StringWriter fileWriter = new StringWriter();

        JsonLayout fileLayout = new JsonLayout();
        fileLayout.activateOptions();

        FileAppender fileAppender = spy(new FileAppender());
        doNothing().when(fileAppender).activateOptions();
        fileAppender.setWriter(fileWriter);
        fileAppender.setFile("/tmp/logger.log");
        fileAppender.setLayout(fileLayout);
        fileAppender.activateOptions();

        logger.addAppender(fileAppender);

        Logger testLogger = Logger.getLogger(getClass());
        testLogger.setLevel(Level.INFO);

        testLogger.info("Hello World!");
        with(fileWriter.toString())
            .assertThat("$.path", equalTo(new File(fileAppender.getFile()).getCanonicalPath()));
    }

    @Test
    public void testMultipleParentLoggersSourcePath() throws Exception {
        logger.removeAllAppenders();

        // for the file appender there must be log file path in the json
        StringWriter fileWriter = new StringWriter();

        JsonLayout fileLayout = new JsonLayout();
        fileLayout.activateOptions();

        FileAppender fileAppender = spy(new FileAppender());
        doNothing().when(fileAppender).activateOptions();
        fileAppender.setWriter(fileWriter);
        fileAppender.setFile("/tmp/logger.log");
        fileAppender.setLayout(fileLayout);
        fileAppender.activateOptions();

        logger.addAppender(fileAppender);

        Logger testLogger = Logger.getLogger(getClass());
        testLogger.setLevel(Level.INFO);
        testLogger.setAdditivity(true);

        Logger packageLogger = Logger.getLogger(getClass().getPackage().getName());
        packageLogger.setLevel(Level.INFO);
        packageLogger.setAdditivity(true);

        testLogger.info("Hello World");

        with(fileWriter.toString())
            .assertThat("$.path", equalTo(new File(fileAppender.getFile()).getCanonicalPath()));
    }


    @Test
    public void testEscape() throws Exception {
        logger.info("H\"e\\l/\nl\ro\u0000W\bo\tr\fl\u0001d");

        with(consoleWriter.toString())
            .assertThat("$.message", equalTo("H\"e\\l/\nl\ro\u0000W\bo\tr\fl\u0001d"));
    }

}

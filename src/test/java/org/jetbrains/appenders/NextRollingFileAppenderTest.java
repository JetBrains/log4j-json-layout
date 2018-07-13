package org.jetbrains.appenders;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Eugene Petrenko (eugene.petrenko@gmail.com)
 */
public class NextRollingFileAppenderTest {
  private File home;
  private NextRollingFileAppender appender;

  @Before
  public void before() throws IOException {
    home = File.createTempFile("aaa", "bbb");
    Paths.delete(home);
    //noinspection ResultOfMethodCallIgnored
    home.mkdirs();
    Assert.assertTrue(home.isDirectory());

    appender = new NextRollingFileAppender();
    appender.setLayout(new JsonLayout());
    appender.setMaxBackupIndex(5);
    appender.setMaximumFileSize(4);
    appender.setFile(new File(home, "log").getPath());
  }

  private void initAppender() {

    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(appender);

    appender.activateOptions();
  }

  @After
  public void after() {
    if (home != null) {
      Paths.delete(home);
    }
  }

  @Test
  public void test_roll() {
    initAppender();

    Logger.getRootLogger().warn("aaa");
    Logger.getRootLogger().warn("bbb");

    assertFiles("log.1", "log.2", "log.3");
  }

  @Test
  public void test_should_not_have_too_huge_index() {
    appender.setMaxBackupIndex(2);
    initAppender();

    for (int i = 0; i < 1002; i++) {
      Logger.getRootLogger().warn("aaa" + i);
    }
    Logger.getRootLogger().warn("bbb");


    final Set<String> names = dumpFiles();
    Assert.assertTrue(home.list().length <= 3);

    //make sure names are not too long
    for (String name : names) {
      Assert.assertTrue(name.matches("log\\.\\d"));
    }
  }

  @Test
  public void test_should_work_if_all_files_created() throws IOException {
    appender.setMaxBackupIndex(1);
    file("log.1", "log.2", "log.3", "log.4", "log.5", "log.6", "log.7", "log.8", "log.9", "log.10", "log.11", "log.12", "log.13");
    initAppender();

    Logger.getRootLogger().warn("bbb");
    assertFiles("log.1", "log.14");
  }

  @Test
  public void test_roll_similar_size() {
    appender.setMaximumFileSize(3);
    appender.setMaxBackupIndex(5);

    initAppender();

    //every message deserves a standalone file
    Logger.getRootLogger().warn("aaa");
    Logger.getRootLogger().warn("aaa");
    Logger.getRootLogger().warn("aaa");
    Logger.getRootLogger().warn("aaa");
    Logger.getRootLogger().warn("aaa");

    Assert.assertTrue(home.list().length <= 6);
  }

  @Test
  public void test_does_not_uses_existing_files() throws IOException {
    file("log.1", "log.2", "log.3");
    initAppender();

    Logger.getRootLogger().warn("aaa");
    assertFiles("log.1", "log.2", "log.3", "log.4", "log.5");
  }

  @Test
  public void test_with_extension() throws IOException {
    appender.setFileExtension(".json");
    initAppender();

    Logger.getRootLogger().warn("aaa");
    Logger.getRootLogger().warn("aaa");
    Logger.getRootLogger().warn("aaa");

    assertFiles("log.1.json", "log.2.json", "log.3.json", "log.4.json");
  }


  private void assertFiles(String... files) {
    final Set<String> actual = dumpFiles();

    for (String file : files) {
      Assert.assertTrue("File " + file + " should exist", new File(home, file).isFile());
      actual.remove(file);
    }

    Assert.assertTrue("" + actual, actual.isEmpty());
  }

  private Set<String> dumpFiles() {
    System.out.println("Files in the directory: ");
    final Set<String> actual = new TreeSet<String>();
    for (String file : home.list()) {
      actual.add(file);
      System.out.println("  " + file);
    }
    return actual;
  }

  private void file(String... names) throws IOException {
    for (String name : names) {
      //noinspection ResultOfMethodCallIgnored
      new File(home, name).createNewFile();
    }
  }

}

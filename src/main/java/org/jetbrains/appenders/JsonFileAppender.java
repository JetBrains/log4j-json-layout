package org.jetbrains.appenders;

/**
 * @author Eugene Petrenko (eugene.petrenko@gmail.com)
 *
 * A simplistic appender to simply souport JSON logging
 */
public class JsonFileAppender extends NextRollingFileAppender {
  {
    setLayout(new JsonLayout());
    setMaximumFileSize(10 * 1024 * 1024);
    setFileExtension(".json");
    setMaxBackupIndex(10);
  }
}

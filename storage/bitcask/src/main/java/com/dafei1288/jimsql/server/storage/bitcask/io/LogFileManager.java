package com.dafei1288.jimsql.server.storage.bitcask.io;

import com.dafei1288.jimsql.common.Config;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LogFileManager {

  public static final String FILE_SUFFIX = ".data";

  private File activeFile;

  private List<File> olderFiles;

  private File parentPath;

  public File getActiveFile() {
    return activeFile;
  }

  public void setActiveFile(File activeFile) {
    this.activeFile = activeFile;
  }

  public List<File> getOlderFiles() {
    return olderFiles;
  }

  public void setOlderFiles(List<File> olderFiles) {
    this.olderFiles = olderFiles;
  }

  public File getParentPath() {
    return parentPath;
  }

  public void setParentPath(File parentPath) {
    this.parentPath = parentPath;
  }



  public static LogFileManager start(String datapath){
    LogFileManager logFileManager = new LogFileManager();
    logFileManager.setParentPath(new File(datapath));
    File[] files = logFileManager.getParentPath().listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(FILE_SUFFIX);
      }
    });

    List<File> allFiles = Arrays.stream(files).sorted((a,b)->{
      String f1 = a.getName();
      String f2 = b.getName();
      return f2.compareTo(f1);
    }).collect(Collectors.toList());

    logFileManager.activeFile = allFiles.get(0);
    logFileManager.olderFiles = allFiles.subList(1,allFiles.size());
    return logFileManager;
  }

  public static LogFileManager start() throws IOException {
    return start(Config.getConfig().getDatadir());
  }



  @Override
  public String toString() {
    return "LogFile{" +
        "activeFile=" + activeFile +
        ", olderFiles=" + olderFiles +
        ", parentPath=" + parentPath +
        '}';
  }
}

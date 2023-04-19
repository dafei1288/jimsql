package com.dafei1288.jimsql.server.storage.bitcask.io;

//import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

class LogFileTest {

  public static void main(String[] args) throws IOException {
    LogFileManager l = LogFileManager.start();
    System.out.println(l.toString());
  }
}
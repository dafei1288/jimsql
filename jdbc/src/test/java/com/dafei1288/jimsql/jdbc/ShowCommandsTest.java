package com.dafei1288.jimsql.jdbc;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

public class ShowCommandsTest {
  private static boolean isServerUp() {
    try (Socket s = new Socket("127.0.0.1", 8821)) { return true; } catch (Exception e) { return false; }
  }

  @Test
  public void legacyShowMetadataShapes() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (legacy)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=legacy");
         Statement st = conn.createStatement()) {
      // SHOW DATABASES
      try (ResultSet rs = st.executeQuery("SHOW DATABASES")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount());
        assertEquals("database", md.getColumnLabel(1).toLowerCase());
      }
      // SHOW TABLES
      try (ResultSet rs = st.executeQuery("SHOW TABLES")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount());
        assertEquals("table", md.getColumnLabel(1).toLowerCase());
      }
      // DESCRIBE
      try (ResultSet rs = st.executeQuery("DESCRIBE user")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("field", md.getColumnLabel(1));
        assertEquals("type", md.getColumnLabel(2));
      }
      // SHOW CREATE TABLE
      try (ResultSet rs = st.executeQuery("SHOW CREATE TABLE user")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("Table", md.getColumnLabel(1));
        assertEquals("Create Table", md.getColumnLabel(2));
      }
    }
  }

  @Test
  public void jspv1ShowMetadataShapes() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement()) {
      // SHOW DATABASES
      try (ResultSet rs = st.executeQuery("SHOW DATABASES")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount());
        assertEquals("database", md.getColumnLabel(1).toLowerCase());
      }
      // SHOW TABLES
      try (ResultSet rs = st.executeQuery("SHOW TABLES")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount());
        assertEquals("table", md.getColumnLabel(1).toLowerCase());
      }
      // DESCRIBE
      try (ResultSet rs = st.executeQuery("DESCRIBE user")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("Field", md.getColumnLabel(1));
        assertEquals("Type", md.getColumnLabel(2));
      }
      // SHOW CREATE TABLE
      try (ResultSet rs = st.executeQuery("SHOW CREATE TABLE user")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("Table", md.getColumnLabel(1));
        assertEquals("Create Table", md.getColumnLabel(2));
      }
    }
  }
}
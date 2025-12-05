package com.dafei1288.jimsql.jdbc;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

public class JspV1AggregateTest {
  private static boolean isServerUp() {
    try (Socket s = new Socket("127.0.0.1", 8821)) { return true; } catch (Exception e) { return false; }
  }

  @Test
  public void sumAvgMinMax_noGroup() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT SUM(age), AVG(age), MIN(age), MAX(age) FROM user")) {
      assertTrue(rs.next());
      assertDoesNotThrow(() -> rs.getString("sum_age"));
      assertDoesNotThrow(() -> rs.getString("avg_age"));
      assertDoesNotThrow(() -> rs.getString("min_age"));
      assertDoesNotThrow(() -> rs.getString("max_age"));
    }
  }

  @Test
  public void groupBy_count_having() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT age, COUNT(*) FROM user GROUP BY age HAVING count > 0")) {
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(2, md.getColumnCount());
      assertEquals("age", md.getColumnLabel(1).toLowerCase());
      assertEquals("count", md.getColumnLabel(2).toLowerCase());
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        assertTrue(rs.getLong("count") >= 0);
      }
      assertTrue(rowCount >= 0);
    }
  }

  @Test
  public void groupBy_sumAvgMinMax() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT age, SUM(age), AVG(age), MIN(age), MAX(age) FROM user GROUP BY age")) {
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(5, md.getColumnCount());
      assertEquals("age", md.getColumnLabel(1).toLowerCase());
      assertEquals("sum_age", md.getColumnLabel(2).toLowerCase());
      assertEquals("avg_age", md.getColumnLabel(3).toLowerCase());
      assertEquals("min_age", md.getColumnLabel(4).toLowerCase());
      assertEquals("max_age", md.getColumnLabel(5).toLowerCase());
      int rows = 0; while (rs.next()) { rows++; }
      assertTrue(rows >= 0);
    }
  }

  @Test
  public void minMax_stringColumn() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT MIN(name), MAX(name) FROM user")) {
      assertTrue(rs.next());
      assertDoesNotThrow(() -> rs.getString("min_name"));
      assertDoesNotThrow(() -> rs.getString("max_name"));
    }
  }
}
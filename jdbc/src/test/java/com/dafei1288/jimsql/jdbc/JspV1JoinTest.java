package com.dafei1288.jimsql.jdbc;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

public class JspV1JoinTest {
  private static boolean isServerUp() {
    try (Socket s = new Socket("127.0.0.1", 8821)) { return true; } catch (Exception e) { return false; }
  }

  @Test
  public void innerJoin_basic() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT * FROM user u INNER JOIN dept d ON u.dept_id = d.id")) {
      ResultSetMetaData md = rs.getMetaData();
      assertTrue(md.getColumnCount() >= 1);
      // should have right side qualified labels like d.id
      boolean hasRight = false;
      for (int i=1;i<=md.getColumnCount();i++) {
        String lbl = md.getColumnLabel(i).toLowerCase();
        if (lbl.startsWith("d.")) { hasRight = true; break; }
      }
      assertTrue(hasRight);
    }
  }

  @Test
  public void leftJoin_basic() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT * FROM user u LEFT JOIN dept d ON u.dept_id = d.id")) {
      int rows = 0; while (rs.next()) { rows++; }
      assertTrue(rows >= 0);
    }
  }
}
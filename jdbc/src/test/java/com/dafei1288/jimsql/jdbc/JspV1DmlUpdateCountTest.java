package com.dafei1288.jimsql.jdbc;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class JspV1DmlUpdateCountTest {

  private static boolean isServerUp() {
    try (Socket s = new Socket("127.0.0.1", 8821)) { return true; } catch (Exception e) { return false; }
  }

  @Test
  public void updateAndDeleteReturnCounts() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement()) {
      int u = st.executeUpdate("UPDATE test.user SET age = 23 WHERE id = 1");
      assertTrue(u >= 0);
      int d = st.executeUpdate("DELETE FROM test.user WHERE id = -999999");
      assertTrue(d >= 0);
    }
  }
}
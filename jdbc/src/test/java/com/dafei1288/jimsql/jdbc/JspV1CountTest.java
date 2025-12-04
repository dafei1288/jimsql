package com.dafei1288.jimsql.jdbc;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

public class JspV1CountTest {
  private static boolean isServerUp() { try (Socket s = new Socket("127.0.0.1", 8821)) { return true; } catch (Exception e) { return false; } }

  @Test
  public void countAll() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (jspv1)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=jspv1");
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM user")) {
      assertTrue(rs.next());
      int c = rs.getInt("count");
      assertTrue(c >= 0);
    }
  }
}
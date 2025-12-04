package com.dafei1288.jimsql.jdbc;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class LegacyInsertValuesTest {
  private static boolean isServerUp() { try (Socket s = new Socket("127.0.0.1", 8821)) { return true; } catch (Exception e) { return false; } }

  @Test
  public void insertValues_thenCleanup() throws Exception {
    Assumptions.assumeTrue(isServerUp(), "requires local server on 127.0.0.1:8821 (legacy)");
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:jimsql://127.0.0.1:8821/test?protocol=legacy");
         Statement st = conn.createStatement()) {
      int n1 = st.executeUpdate("INSERT INTO test.user (id,name,age) VALUES (999997,'tmp2',2)");
      assertTrue(n1 >= 0);
      int n2 = st.executeUpdate("DELETE FROM test.user WHERE id = 999997");
      assertTrue(n2 >= 0);
    }
  }
}
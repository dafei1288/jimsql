package com.dafei1288.jimsql.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for PreparedStatement querying user table.
 * This test requires a running jimsql server (default localhost:8821, db=test)
 * and sample data containing a row (id=1, name='jacky').
 * Enable by setting env JIMSQL_IT=true.
 */
public class JqPreparedStatementIT {

    @Test
    public void selectUserByIdAndName() throws Exception {
//        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("JIMSQL_IT")),
//                "Set JIMSQL_IT=true to enable integration test");

        Properties info = new Properties();
        info.setProperty("host", System.getProperty("jimsql.host", "127.0.0.1"));
        info.put("port", Integer.getInteger("jimsql.port", 8821));
        info.setProperty("db", System.getProperty("jimsql.db", "test"));
//        info.setProperty("protocol", System.getProperty("jimsql.protocol", "jspv1"));

        try (JqConnection conn = new JqConnection(info)) {
            PreparedStatement ps = conn.prepareStatement("select id,name from user where id=?");
            ps.setInt(1, 1);
//            ps.setString(2, "jacky");
            try (ResultSet rs = ps.executeQuery()) {
                assertNotNull(rs);
                assertTrue(rs.next(), "expected at least one row");
                // Validate returned columns are consistent
                String id = rs.getString("id");
                String name = rs.getString("name");
                String age = rs.getString("age");
                assertEquals("1", id);
                assertEquals("jacky", name);

                System.out.println(id + " " + name + " age = "+age);
            }
        }
    }
}
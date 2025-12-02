package com.dafei1288.jimsql.common;

import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

public class JqResultSetMetaDataTest {
  @Test
  public void testColumnIndexIsOneBased() throws Exception {
    LinkedHashMap<String,JqColumnResultSetMetadata> m = new LinkedHashMap<>();
    JqColumnResultSetMetadata c1 = new JqColumnResultSetMetadata(); c1.setIndex(1); c1.setLabelName("id"); c1.setTableName("t"); c1.setColumnType(java.sql.Types.INTEGER); c1.setClazzStr("java.lang.Integer");
    JqColumnResultSetMetadata c2 = new JqColumnResultSetMetadata(); c2.setIndex(2); c2.setLabelName("name"); c2.setTableName("t"); c2.setColumnType(java.sql.Types.VARCHAR); c2.setClazzStr("java.lang.String");
    m.put("id", c1); m.put("name", c2);
    JqResultSetMetaData meta = new JqResultSetMetaData(m);

    assertEquals("id", meta.getColumnLabel(1));
    assertEquals("name", meta.getColumnLabel(2));

    assertEquals("id", meta.getColumnName(1));
    assertEquals("name", meta.getColumnName(2));

    assertEquals("t", meta.getTableName(1));
    assertEquals(java.sql.Types.VARCHAR, meta.getColumnType(2));
    assertEquals("java.lang.String", meta.getColumnTypeName(2));
  }
}

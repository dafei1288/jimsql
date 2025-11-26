package com.dafei1288.jimsql.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JqDriver implements Driver {

  private static final JqDriver INSTANCE = new JqDriver();
  private static final String DEFAULT_URL = "jdbc:jimsql:";
  private static final String PATTERN  = "^jdbc:jimsql://([a-zA-Z0-9_\\.]+):([0-9]+)/([a-zA-Z0-9_\\.]+)(\\?([^#]+))?$";
  private static String DATADIR = "/tmp";
  private static final ThreadLocal<Connection> DEFAULT_CONNECTION =
      new ThreadLocal<>();
  private static Pattern urlPattern = Pattern.compile(PATTERN);
  private static boolean registered;

  static {
    load();
  }

  private static JqDriver load() {
    try {
      if (!registered) {
        registered = true;
        DriverManager.registerDriver(INSTANCE);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return INSTANCE;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {

    Matcher m = urlPattern.matcher(url);
    String host = "";
    int port = 0;
    String db = "";
    String query = null;

    if(m.find()){
      host = m.group(1);
      port = Integer.parseInt(m.group(2));
      db = m.group(3);
      query = m.groupCount() >= 5 ? m.group(5) : null;

      info.put("host",host);
      info.put("port",port);
      info.put("db",db);
      info.put("url",url);
      String proto = System.getProperty("jimsql.protocol");
      if (proto != null) {
        info.put("protocol", proto);
      }
      if (query != null && !query.isEmpty()) {
        java.util.Map<String,String> q = parseQuery(query);
        for (java.util.Map.Entry<String,String> e : q.entrySet()) {
          info.put(e.getKey(), e.getValue());
        }
        proto = q.get("protocol");
        if (proto == null) proto = q.get("jimsql.protocol");
        if (proto != null) info.put("protocol", proto);
        if (q.get("user") != null) info.put("user", q.get("user"));
        if (q.get("password") != null) info.put("password", q.get("password"));
      }
      String protoSys = System.getProperty("jimsql.protocol");
      if (protoSys != null && !protoSys.isEmpty()) {
        info.put("protocol", protoSys);
      }
    }else{
      throw new SQLException("url error please makesure the url is useable ");
    }
    JqConnection jq = null;

    try {
      jq = new JqConnection(info);
    } catch (IOException e) {
      throw new SQLException(String.format("cannot connect to server %s on port %d",host,port));
    }

    return jq;
  }

  private static java.util.Map<String,String> parseQuery(String q) {
    java.util.Map<String,String> m = new java.util.HashMap<>();
    String[] parts = q.split("&");
    for (String p : parts) {
      if (p.isEmpty()) continue;
      int idx = p.indexOf('=');
      String k; String v;
      if (idx > 0) {
        k = p.substring(0, idx);
        v = p.substring(idx+1);
      } else { k = p; v = ""; }
      try {
        k = java.net.URLDecoder.decode(k, java.nio.charset.StandardCharsets.UTF_8);
        v = java.net.URLDecoder.decode(v, java.nio.charset.StandardCharsets.UTF_8);
      } catch (Exception ignored) {}
      m.put(k, v);
    }
    return m;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    Matcher m = urlPattern.matcher(url);
    return m.find();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}

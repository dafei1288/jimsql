package com.dafei1288.jimsql.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class JqDriver implements Driver {

  private static final JqDriver INSTANCE = new JqDriver();
  private static final String DEFAULT_URL = "jdbc:jimsql:";
  private static String DATADIR = "/tmp";
  private static final ThreadLocal<Connection> DEFAULT_CONNECTION =
      new ThreadLocal<>();

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
    System.out.println(url);
    System.out.println(info);
    String filepath = url.replace(DEFAULT_URL,"");
    System.out.println(filepath);
    return new JqConnection(filepath);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    System.out.println("acceptsURL ? "+url);
    return false;
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

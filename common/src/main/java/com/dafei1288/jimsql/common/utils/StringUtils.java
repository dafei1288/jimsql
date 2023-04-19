package com.dafei1288.jimsql.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
  /**
   * 根据url获取库名
   * @param jdbcUrl
   * @return
   */
  public static String getDbnameByUrl(String jdbcUrl) {
    Pattern p = Pattern.compile("jdbc:(?<db>\\w+):.*((//)|@)(?<host>.+):(?<port>\\d+)(/|(;DatabaseName=)|:)(?<dbName>\\w+)\\?((?)(?<key>.+)=(?<value>.+))?.*");
    Matcher m = p.matcher(jdbcUrl);
    String res = null;
    if(m.find()) {
      res = m.group("dbName");

      for(int i=0;i<=m.groupCount();i++) {
        System.out.println(i+" ==> "+m.group(i));
      }
    }

//    m.reset().results().forEach(it->System.out.println(it.group()));
    return res;
  }

  public static void main(String[] args) {
    String url = "jdbc:mysql://localhost:3306/test?encoding=aaa";
    System.out.println(getDbnameByUrl(url));
  }
}

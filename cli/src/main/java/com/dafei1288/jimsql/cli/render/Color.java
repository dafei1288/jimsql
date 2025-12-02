package com.dafei1288.jimsql.cli.render;

public final class Color {
  public enum Mode { AUTO, ALWAYS, NEVER }
  private static Mode mode = Mode.ALWAYS;

  public static void setMode(Mode m) { mode = m; }
  public static Mode toMode(Enum<?> e) { return Mode.valueOf(e.name().toUpperCase()); }

  private static boolean enabled() {
    if (mode == Mode.ALWAYS) return true;
    if (mode == Mode.NEVER) return false;
    // AUTO: enable only when console present and NO_COLOR not set
    boolean tty = System.console() != null;
    boolean noColor = System.getenv("NO_COLOR") != null;
    return tty && !noColor;
  }

  private static String code(String s) { return enabled() ? s : ""; }

  private static final String RESET = "\u001B[0m";
  private static final String BOLD = "\u001B[1m";
  private static final String DIM = "\u001B[2m";
  private static final String RED = "\u001B[31m";
  private static final String GREEN = "\u001B[32m";
  private static final String YELLOW = "\u001B[33m";
  private static final String BLUE = "\u001B[34m";
  private static final String CYAN = "\u001B[36m";

  public static String bold(String s) { return code(BOLD) + s + code(RESET); }
  public static String dim(String s) { return code(DIM) + s + code(RESET); }
  public static String red(String s) { return code(RED) + s + code(RESET); }
  public static String green(String s) { return code(GREEN) + s + code(RESET); }
  public static String yellow(String s) { return code(YELLOW) + s + code(RESET); }
  public static String blue(String s) { return code(BLUE) + s + code(RESET); }
  public static String cyan(String s) { return code(CYAN) + s + code(RESET); }
}

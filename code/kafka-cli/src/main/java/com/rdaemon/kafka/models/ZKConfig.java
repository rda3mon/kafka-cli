package com.rdaemon.kafka.models;

public class ZKConfig {
  private static int sessionTimeoutMs = 10 * 1000;
  private static int connectionTimeoutMs = 8 * 1000;

  public static int getSessionTimeoutMs() {
    return sessionTimeoutMs;
  }

  public static void setSessionTimeoutMs(int sessionTimeoutMs) {
    ZKConfig.sessionTimeoutMs = sessionTimeoutMs;
  }

  public static int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  public static void setConnectionTimeoutMs(int connectionTimeoutMs) {
    ZKConfig.connectionTimeoutMs = connectionTimeoutMs;
  }
}

package net.sinchii.simpara;

public class SConfig {
  public final static String SPUT_CONTEXT = "/put";
  public final static int SCLIENT_PORT = 60000;
  public final static int DEFAULT_SPEEP_MSECOND = 100;

  public final static String PROP_SCLIENT_HOST = "simpara.hostname";
  public final static String PROP_SCLIENT_PORT = "simpara.port";

  public enum SCounter {
    OUTPUT_RECORDS,
  }
}

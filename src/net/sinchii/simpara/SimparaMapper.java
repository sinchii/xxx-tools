package net.sinchii.simpara;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class SimparaMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, NullWritable, NullWritable> {

  private String toClientName;
  private String toClientPort;
  private String url;
  private List<String> result;
  private Counter counter;

  public void setup(Context context)
    throws IOException, InterruptedException {

    toClientName = context.getConfiguration().get(SConfig.PROP_SCLIENT_HOST);
    toClientPort = context.getConfiguration().get(SConfig.PROP_SCLIENT_PORT);

    url = "http://" + toClientName + ":" + toClientPort + SConfig.SPUT_CONTEXT + "?";
    result = new ArrayList<String>();

    counter = context.getCounter(SConfig.SCounter.OUTPUT_RECORDS);
  }


  public void sendResult() throws IOException, InterruptedException {
    for (String s : result) {
      while (true) {
        URL u = new URL(url + "j=" + s);
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        int code = conn.getResponseCode();
        if (code == 200) {
          break;
        }
        Thread.sleep(SConfig.DEFAULT_SPEEP_MSECOND);
      }
    }
    counter.increment(result.size());
    result.clear();
  }

}

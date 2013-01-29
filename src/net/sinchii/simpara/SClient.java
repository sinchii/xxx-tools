package net.sinchii.simpara;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SClient extends Configured implements Tool {

  private Job job;
  private static final Log LOG = LogFactory.getLog(SClient.class);
  HttpServer server;

  @Override
  public int run(String[] arg0) throws Exception {
    server = new HttpServer("sclient", "localhost", SConfig.SCLIENT_PORT, true, getConf());
    server.addInternalServlet("put", SConfig.SPUT_CONTEXT, SputServlet.class, true);
    server.start();
    LOG.info("sclient start port : " + SConfig.SCLIENT_PORT);

    job = new Job();
    job.submit();

    server.stop();
    LOG.info("sclient stop");

    return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Configuration(), new SClient(), args);
    System.exit(ret);
  }

}

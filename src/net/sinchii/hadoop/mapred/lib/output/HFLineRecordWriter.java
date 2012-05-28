package net.sinchii.hadoop.mapred.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;

public class HFLineRecordWriter extends AbstractHFLineRecordWriter {

  public HFLineRecordWriter(DataOutputStream out) throws IOException {
    super(out, "\t");
  }

  public HFLineRecordWriter(DataOutputStream out, String kvSeparator)
    throws IOException {
    super(out, kvSeparator, "UTF-8");
  }

  public HFLineRecordWriter(DataOutputStream out, String kvSeparator, String encoding)
    throws IOException {
    super(out, kvSeparator, encoding);
  }

  @Override
  public String header() {
    //TODO
    return null;
  }

  @Override
  public String footer() {
    //TODO
    return null;
  }

}

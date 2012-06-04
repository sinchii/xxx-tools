package net.sinchii.hadoop.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class AbstractHFLineRecordWriter extends RecordWriter<Text, Text> {

  protected String encoding = "UTF-8";
  protected byte[] newline;

  protected DataOutputStream out;
  protected byte[] keyValueSeparator;

  public AbstractHFLineRecordWriter(DataOutputStream out)
    throws IOException {
    this(out, "\t");
  }

  public AbstractHFLineRecordWriter(DataOutputStream out, String kvSeparator)
    throws IOException {
    this(out, kvSeparator, "UTF-8");
  }

  public AbstractHFLineRecordWriter(DataOutputStream out, String kvSeparator, String encoding)
    throws IOException {
    this.out = out;
    try {
      this.keyValueSeparator = kvSeparator.getBytes(encoding);
      this.newline = "\n".getBytes(encoding);
    } catch (UnsupportedEncodingException uee) {
      throw new IllegalArgumentException("can't find " + encoding + " encoding");
    }
    writeHeaderFooter(header());

  }

  public abstract String header();
  public abstract String footer();

  protected void writeHeaderFooter(String str) throws IOException {
    if (str == null) {
      return;
    }
    out.write(str.getBytes(encoding));
  }

  @Override
  public synchronized void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
    writeHeaderFooter(footer());
    out.close();

  }

  @Override
  public void write(Text key, Text value) throws IOException,
      InterruptedException {
    boolean nullKey = key.getBytes() == null;
    boolean nullValue = value.getBytes() == null;

    if (nullKey && nullValue) {
      return;
    }
    if (!nullKey) {
      out.write(key.toString().getBytes(encoding));
    }
    if (!(nullKey || nullValue)) {
      out.write(keyValueSeparator);
    }
    if (!nullValue) {
      out.write(value.toString().getBytes(encoding));
    }
    out.write(newline);
  }
}

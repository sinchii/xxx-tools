package net.sinchii.hadoop.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class HeaderFooterTextOutputFormat extends FileOutputFormat<Text, Text> {

  @Override
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {

    Configuration config = context.getConfiguration();
    String kvSeparator = config.get("mapred.hftextoutputformat.separator", "\t");
    String encoding = config.get("mapred.hftextoutputformat.encoding", "UTF-8");

    CompressionCodec codec = null;
    String extension = "";
    boolean isCompressed = getCompressOutput(context);
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass =
          getOutputCompressorClass(context, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, config);
      extension = codec.getDefaultExtension();
    }

    Path file = getDefaultWorkFile(context, extension);
    FileSystem fs = file.getFileSystem(config);
    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new HFLineRecordWriter(fileOut, kvSeparator, encoding);
    } else {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new HFLineRecordWriter(
          new DataOutputStream(codec.createOutputStream(fileOut)), kvSeparator, encoding);
    }
  }

}

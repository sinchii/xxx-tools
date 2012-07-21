package net.sinchii.hadoop.hdfs.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSPutToSnappyFile extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.println("Usage HDFSPutToSnappyFile inputfile outputfile");
      return -1;
    }

    LocalFileSystem srcFs =
        (LocalFileSystem) FileSystem.get(URI.create("file:///"), getConf());
    FileSystem dstFs = FileSystem.get(getConf());

    Path inputFile = new Path(args[0]);
    Class<? extends CompressionCodec> codecClass = SnappyCodec.class;

    CompressionCodec codec =
        (CompressionCodec) ReflectionUtils.newInstance(codecClass, getConf());

    Path outputFile = new Path(args[1] + codec.getDefaultExtension());

    InputStream in = null;
    OutputStream out = null;

    try {
      in = srcFs.open(inputFile);
      out = codec.createOutputStream(dstFs.create(outputFile));
      IOUtils.copyBytes(in, out, getConf());
    } catch (IOException e) {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
    }

    return 0;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Configuration(), new HDFSPutToSnappyFile(), args);
    System.exit(ret);
  }

}

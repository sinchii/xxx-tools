package net.sinchii.hadoop.hdfs.tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSPutToSequenceFile extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.println("Usage: HDFSPutToSequenceFile inputfile outputfile");
      return -1;
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    // get Local FileSystem
    LocalFileSystem srcFs = (LocalFileSystem) FileSystem.get(URI.create("file:///"), getConf());

    BufferedReader reader = new BufferedReader(new InputStreamReader(srcFs.open(inputPath)));

    SequenceFile.Writer writer =
        SequenceFile.createWriter(outputPath.getFileSystem(getConf()),
            getConf(), outputPath, LongWritable.class, Text.class);

    LongWritable key = new LongWritable();
    Text value = new Text();

    long lnumber = 1;
    String line;
    while ((line = reader.readLine()) != null) {
      key.set(++lnumber);
      value.set(line);
      writer.append(key, value);
    }
    writer.close();

    return 0;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Configuration(), new HDFSPutToSequenceFile(), args);
    System.exit(ret);
  }

}

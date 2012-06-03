package net.sinchii.hadoop.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import net.sinchii.hadoop.util.HadoopFileUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileMerger extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    String[] inputPath = args[0].split(",");
    if (inputPath.length == 0) {
      return -1;
    }

    Path[] inputPaths = new Path[inputPath.length-1];
    for (int i = 0; i < inputPath.length; i++) {
      inputPaths[i] = new Path(inputPath[i]);
    }
    Path outputPath = new Path(args[1]);

    FileSystem fs = FileSystem.get(getConf());
    InputStream is = null;

    FSDataOutputStream fos = fs.create(outputPath);

    for (int i = 0; i < inputPath.length; i++) {
      switch(HadoopFileUtil.fileType(fs.open(inputPaths[i]))) {
        case HadoopFileUtil.GZIPFILE:
          is = new GZIPInputStream(fs.open(inputPaths[i]));
          break;
        case HadoopFileUtil.SEQFILE:
          is = new SeqFileInputStream(fs, fs.getFileStatus(inputPaths[i]));
          break;
        case HadoopFileUtil.TEXTFILE:
          is = fs.open(inputPaths[i]);
          break;
      }

      IOUtils.copyBytes(is, fos, getConf(), false);
      is.close();
    }
    fos.close();
    
    return 0;
  }

  private class SeqFileInputStream extends InputStream {
    SequenceFile.Reader r;
    WritableComparable key;
    Writable val;

    DataInputBuffer inbuf;
    DataOutputBuffer outbuf;

    public SeqFileInputStream(FileSystem fs, FileStatus f) throws IOException {
      r = new SequenceFile.Reader(fs, f.getPath(), getConf());
      key = ReflectionUtils.newInstance(r.getKeyClass().asSubclass(WritableComparable.class),
                                        getConf());
      val = ReflectionUtils.newInstance(r.getValueClass().asSubclass(Writable.class),
                                        getConf());
      inbuf = new DataInputBuffer();
      outbuf = new DataOutputBuffer();
    }

    public int read() throws IOException {
      int ret;
      if (null == inbuf || -1 == (ret = inbuf.read())) {
        if (!r.next(key, val)) {
          return -1;
        }
        byte[] tmp = key.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\t');
        tmp = val.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\n');
        inbuf.reset(outbuf.getData(), outbuf.getLength());
        outbuf.reset();
        ret = inbuf.read();
      }
      return ret;
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Configuration(), new FileMerger(), args);
    System.exit(ret);
  }

}

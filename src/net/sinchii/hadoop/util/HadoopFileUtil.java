package net.sinchii.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

public class HadoopFileUtil {

  public final static int GZIPFILE = 1;
  public final static int SEQFILE = 2;
  public final static int TEXTFILE = 3;

  public static int fileType(FSDataInputStream fis) throws IOException {

    int ret = 0;

    switch (fis.readShort()) {
    case 0x1f8b:
      fis.seek(0);
      ret = GZIPFILE;
      break;
    case 0x5345:
      if (fis.readByte() == 'Q') {
        ret = SEQFILE;
      } else {
        ret = TEXTFILE;
      }
      break;
    }
    fis.close();

    return ret;
  }

}

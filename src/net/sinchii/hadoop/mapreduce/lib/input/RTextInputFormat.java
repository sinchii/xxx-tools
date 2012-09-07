package net.sinchii.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

public class RTextInputFormat extends TextInputFormat {

  private static final Log LOG = LogFactory.getLog(RTextInputFormat.class);

  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   *
   */
  protected List<FileStatus> listStatus(JobContext job)
    throws IOException {

    List<FileStatus> result = new ArrayList<FileStatus>();
    Path[] dirs = getInputPaths(job);

    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // get tokens for all the requiered FileSystems.
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
        job.getConfiguration());

    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }

    List<IOException> errors = new ArrayList<IOException>();

    for (int i = 0; i < dirs.length; i++) {
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job.getConfiguration());
      FileStatus[] matches = fs.listStatus(p);

      if (matches == null) {
        errors.add(new IOException("Input path does not exist : " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus f : matches) {
          if (f.isDir()) {
            result.addAll(getInputFileList(f.getPath(), fs));
          } else {
            result.add(f);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }

    LOG.info("Total input paths to process : " + result.size());

    return result;
  }

  /**
   * Generate the list of files in some directories
   * @param p directory path
   * @param fs FileSystem
   * @return
   * @throws IOException
   */
  private List<FileStatus> getInputFileList(Path p, FileSystem fs)
    throws IOException {

    List<FileStatus> r = new ArrayList<FileStatus>();
    FileStatus[] status = fs.listStatus(p);

    if (status == null) {
      return r;
    }

    for (FileStatus f : status) {
      if (f.isDir()) {
        r.addAll(getInputFileList(f.getPath(), fs));
      } else {
        r.add(f);
      }
    }

    return r;
  }
}

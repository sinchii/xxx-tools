package net.sinchii.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Overrided {@link TextInputFormat}.
 * @author sinchii
 *
 */
public class ZeroByteIgnoreTextInputFormat extends TextInputFormat {

  /**
   * Generate the list of files without zero byte
   * and make them into FileSplits.
   * @return the list of FileSplits without zero byte split
   */
  public List<InputSplit> getSplits(JobContext job)
      throws IOException {
		
    // Set splits by FileInputFormat.getSplits()
    List<InputSplit> splits = super.getSplits(job);
    List<InputSplit> newSplits = new ArrayList<InputSplit>();
    
    for (int i = 0; i < splits.size(); i++) {
      FileSplit fs = (FileSplit) splits.get(i);
      
      // Set Not 0 byte FileSplit 
      if (fs.getLength() != 0) {
        newSplits.add(fs);
       }
    }
    return newSplits;
  }
}

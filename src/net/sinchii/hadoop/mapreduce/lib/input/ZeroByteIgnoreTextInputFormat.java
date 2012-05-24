package net.sinchii.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ZeroByteIgnoreTextInputFormat extends TextInputFormat {

	public List<InputSplit> getSplits(JobContext job)
		throws IOException {
		
		List<InputSplit> splits = super.getSplits(job);
		List<InputSplit> newSplits = new ArrayList<InputSplit>();
		
		for (int i = 0; i < splits.size(); i++) {
			FileSplit fs = (FileSplit) splits.get(i);
			if (fs.getLength() != 0) {
				newSplits.add(fs);
			}
		}
		
		return newSplits;
	}
}

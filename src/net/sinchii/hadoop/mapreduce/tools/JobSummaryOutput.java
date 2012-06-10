package net.sinchii.hadoop.mapreduce.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;

public class JobSummaryOutput {

  private String jobID;
  private Job job;

  public JobSummaryOutput(Job job) {
    this.job = job;
    jobID = job.getJobID().toString();
  }

  public JobSummaryOutput(String jobID) {
    job = null;
    this.jobID = jobID;
  }

  public void printJobSummary() throws IOException {
    printJobSummary(jobID);
  }

  public void printJobSummary(String filename) throws IOException {

    // get Job status
    JobConf jobConf = new JobConf(job.getConfiguration());
    JobClient jc = new JobClient(jobConf);

    TaskReport[] mapTasks = jc.getMapTaskReports(JobID.forName(jobID));
    TaskReport[] reduceTasks = jc.getReduceTaskReports(JobID.forName(jobID));

    // print Job status
    File file = new File(filename);
    PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file)));

    for (TaskReport m : mapTasks) {
      pw.print(m.getTaskID().toString() + ","
          + m.getStartTime() + ","
          + m.getFinishTime() + ".");
      for (Counters.Group g : m.getCounters()) {
        for (Counter c : g) {
          pw.print(c.getCounter());
        }
      }
      pw.println();
    }

    for (TaskReport r : reduceTasks) {
      pw.print(r.getTaskID().toString() + ","
          + r.getStartTime() + ","
          + r.getFinishTime() + ".");
      for (Counters.Group g : r.getCounters()) {
        for (Counter c : g) {
          pw.print(c.getCounter());
        }
      }
      pw.println();
    }
    pw.close();
  }
}

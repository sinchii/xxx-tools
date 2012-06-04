package net.sinchii.hadoop.mapreduce.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskReport;
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

  @SuppressWarnings("deprecation")
  public void printJobSummary(String filename) throws IOException {

    // get Job status
    JobConf jobConf = new JobConf(job.getConfiguration());
    JobClient jc = new JobClient(jobConf);

    TaskReport[] mapTasks = jc.getMapTaskReports(jobID);
    TaskReport[] reduceTasks = jc.getReduceTaskReports(jobID);

    // print Job stauts
    File file = new File(filename);
    PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file)));

    for (TaskReport m : mapTasks) {
      pw.print(m.getTaskID().toString() + ","
          + m.getStartTime() + ","
          + m.getFinishTime() + ".");
      // TODO print counter
      pw.println();
    }

    for (TaskReport r : reduceTasks) {
      pw.print(r.getTaskID().toString() + ","
          + r.getStartTime() + ","
          + r.getFinishTime() + ".");

      // TODO print counter
      pw.println();
    }

    pw.close();
  }
}

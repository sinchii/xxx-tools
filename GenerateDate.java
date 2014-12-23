package com.example;

import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

public class GenerateData {

  public static void main(String[] args) throws Exception {
    // TODO 自動生成されたメソッド・スタブ
    FileOutputStream out = new FileOutputStream("h:\\tmp\\sample.out");
    PrintWriter pw = new PrintWriter(out);
    
    for (int i = 0; i < 1000000; i++) {
      for (int j = 0; j < 100; j++) {
        pw.print(RandomStringUtils.randomAlphanumeric(1 + RandomUtils.nextInt(10)) + ",");
      }
      pw.println();
      pw.flush();
    }
    if (pw != null) {
      pw.close();
    }
    if (out != null) {
      out.close();
    }
  }
}

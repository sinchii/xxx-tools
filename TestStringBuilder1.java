/**
 * stringbuilder test case 1
 * @author sinchii
 * 
 * Result 26.542 secs
 */

package com.example;

import java.io.File;
import java.util.Scanner;

public class TestStringBuilder1 {

  public static void main(String[] args) throws Exception {
    Scanner scan = new Scanner(new File("h:\\tmp\\sample.out"));
    scan.useDelimiter("\n");

    long start = System.currentTimeMillis();
    StringBuilder sb = new StringBuilder();
    while (scan.hasNext()) {
      sb.setLength(0);
      String str = scan.next();
      String[] tmp = str.split(",");
      for (int i = 0; i < 60; i++) {
        sb.append(tmp[i]).append(",");
      }
    }
    long end = System.currentTimeMillis();
    
    if (scan != null) {
      scan.close();
    }
    System.out.println("Time : " + (end - start) / 1000.0);
  } 
}

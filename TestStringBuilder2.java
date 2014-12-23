/**
 * stringbuilder test case 2
 * @author sinchii
 * 
 * Result : 20.063 secs
 */
package com.example;

import java.io.File;
import java.util.Scanner;

public class TestStringBuilder2 {

  public static void main(String[] args) throws Exception {
    Scanner scan = new Scanner(new File("h:\\tmp\\sample.out"));
    scan.useDelimiter("\n");

    long start = System.currentTimeMillis();
    StringBuilder sb = new StringBuilder();
    while (scan.hasNext()) {
      sb.setLength(0);
      String str = scan.next();
      int index = 0;
      int comma = 0;
      for (int i = 0; i < str.length(); i++, index++) {
        if (str.charAt(i) == ',') {
          comma++;
          if (comma == 60) {
            index++;
            break;
          }
        }
      }
      sb.append(str.substring(0, index));
    }
    long end = System.currentTimeMillis();
    
    if (scan != null) {
      scan.close();
    }
    System.out.println("Time : " + (end - start) / 1000.0);
  } 
}

/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.planner;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;


public class BenchmarkAcceleratedQuery extends BaseTestQuery {

  public static void main(String[] args) throws Exception {

    createCSV("filter_re2.csv");
    createCSV("parquet_re2.csv");

    try {
      setupDefaultTestCluster();
      testInBenchmark();
      testBatchBenchmark();
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.exit(0);
  }

  public static void testInBenchmark() throws Exception {
    setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, "64000");

    writeBenchmarkName("filter_re2.csv", "IN BENCHMARK");
    writeBenchmarkName("parquet_re2.csv", "IN BENCHMARK");

    int repeats = 10;
    String[] in_sizes = {"1000", "2000", "4000", "8000", "16000", "32000", "64000", "128000", "256000", "512000", "1000000", "2000000", "4000000"};

    for (int i = 0; i < in_sizes.length; i++) {
      System.out.println(in_sizes[i] + ": ");
      String query = "SELECT SUM(\"value\") FROM cp.\"diving/data-" + in_sizes[i].replace("000000", "M") + "-1M.parquet\" WHERE REGEXP_LIKE(\"string\", '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \\t\\n]+[dD][iI][vV][iI][nN][gG][ \\t\\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*')";
      for (int r = 0; r < repeats; r++) {
        System.out.println(String.valueOf(r));
        writeRecordsRepeatsToCSV("filter_re2.csv", in_sizes[i], String.valueOf(r));
        writeRecordsRepeatsToCSV("parquet_re2.csv", in_sizes[i], String.valueOf(r));
        test(query);
      }
    }
  }

  public static void testBatchBenchmark() throws Exception {

    int repeats = 10;
    String query = "SELECT SUM(\"value\") FROM cp.\"diving/data-4M-1M.parquet\" WHERE REGEXP_LIKE(\"string\", '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \\t\\n]+[dD][iI][vV][iI][nN][gG][ \\t\\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*')";
    String[] batch_sizes = {"1000", "2000", "4000", "8000", "16000", "32000", "64000"};

    writeBenchmarkName("filter_re2.csv", "BATCH BENCHMARK");
    writeBenchmarkName("parquet_re2.csv", "BATCH BENCHMARK");

    for (int i = 0; i < batch_sizes.length; i++) {
      System.out.println(batch_sizes[i] + ": ");
      setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, batch_sizes[i]);
      for (int r = 0; r < repeats; r++) {
        System.out.println(String.valueOf(r));
        writeRecordsRepeatsToCSV("filter_re2.csv", batch_sizes[i], String.valueOf(r));
        writeRecordsRepeatsToCSV("parquet_re2.csv", batch_sizes[i], String.valueOf(r));
        test(query);
      }
    }
  }

  // This overwrites old csv files
  private static void createCSV(String filename) {
    try (PrintWriter writer = new PrintWriter(new File(filename))) {

      StringBuilder sb = new StringBuilder();
      sb.append("records");
      sb.append(',');
      sb.append("repeat");
      sb.append(',');
      sb.append("b0");
      sb.append(',');
      sb.append("b1");
      sb.append(',');
      sb.append("b2");
      sb.append(',');
      sb.append("b3");
      sb.append(',');
      sb.append("b4");
      sb.append(',');
      sb.append("b5");

      writer.write(sb.toString());

      System.out.println("New csv file created");

    } catch (FileNotFoundException e) {
      System.out.println(e.getMessage());
    }
  }

  private static void writeRecordsRepeatsToCSV(String filename, String records, String repeat) throws Exception {
    try (PrintWriter writer = new PrintWriter(new FileWriter(filename, true))) {

      StringBuilder sb = new StringBuilder();
      sb.append('\n');
      sb.append(records);
      sb.append(',');
      sb.append(repeat);
      sb.append(',');

      writer.write(sb.toString());

    } catch (FileNotFoundException e) {
      System.out.println(e.getMessage());
    }
  }

  private static void writeBenchmarkName(String filename, String benchmarkName) throws Exception {
    try (PrintWriter writer = new PrintWriter(new FileWriter(filename, true))) {

      StringBuilder sb = new StringBuilder();
      sb.append('\n');
      sb.append(benchmarkName);

      writer.write(sb.toString());

    } catch (FileNotFoundException e) {
      System.out.println(e.getMessage());
    }
  }
}

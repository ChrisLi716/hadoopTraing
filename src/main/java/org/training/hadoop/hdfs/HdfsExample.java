package org.training.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.util.Properties;

public class HdfsExample {

  public static void testMkdirPath(String path) throws Exception {
    FileSystem fs = null;
    try {
      System.out.println("Creating " + path + " on hdfs...");
      Configuration conf = new Configuration();
      // First create a new directory with mkdirs
      Path myPath = new Path(path);
      fs = myPath.getFileSystem(conf);

      fs.mkdirs(myPath);
      System.out.println("Create " + path + " on hdfs successfully.");
    } catch (Exception e) {
      System.out.println("Exception:" + e);
    } finally {
      if(fs != null)
        fs.close();
    }
  }

  public static void testDeletePath(String path) throws Exception {
    FileSystem fs = null;
    try {
      System.out.println("Deleting " + path + " on hdfs...");
      Configuration conf = new Configuration();
      Path myPath = new Path(path);
      fs = myPath.getFileSystem(conf);

      fs.delete(myPath, true);
      System.out.println("Deleting " + path + " on hdfs successfully.");
    } catch (Exception e) {
      System.out.println("Exception:" + e);
    } finally {
      if(fs != null)
        fs.close();
    }
  }

  public static void main(String[] args) {
    try {
      System.setProperty("HADOOP_USER_NAME", "bigdata");
      String path = "hdfs://hadoop:9000/chris/mkdirs-test";
      //String path = "/tmp/mkdirs-test";
      testMkdirPath(path);
      //testDeletePath(path);
    } catch (Exception e) {
      System.out.println("Exceptions:" + e);
    }
    System.out.println("timestamp:" + System.currentTimeMillis());
  }
}

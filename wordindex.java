package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class wordindex {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text strTitle = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      System.err.println("Begin map");
      String line = value.toString();
	  String subline = "";
	  strTitle.set("");
      int i;
	  
	  for (i = 2; i<=line.length(); i++){
	     if (line.substring(i, i-2) == "txt"){
			 strTitle.set(line.substring(2, i));
			 break;
		 }
	  }
	  
	  subline = line.substring(i + 3);
      StringTokenizer tokenizer = new StringTokenizer(subline);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
	System.err.println("xxx: " + word + " " + strTitle);
        output.collect(word, strTitle);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int sum = 0;
	  String strBooklist = "";//new String;
	  strBooklist = "";
	  
      while (values.hasNext()) {
        //sum += values.next().get();
		strBooklist = strBooklist + values.next();
      }
      Text txtBooklist = new Text();
      txtBooklist.set(strBooklist);
      output.collect(key, txtBooklist);
    }

  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(wordindex.class);
    conf.setJobName("wordindex");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}

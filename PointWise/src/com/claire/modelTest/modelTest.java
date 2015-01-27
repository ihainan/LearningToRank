package com.claire.modelTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.claire.learningtorank.pointwise.PointwiseStart;

public class modelTest {
	
	 public static class TestMapper 
     extends Mapper<Object, Text, Text, Text>{
		    
		  public void map(Object key, Text value, Context context
		                  ) throws IOException, InterruptedException {
		    String data = value.toString();
		    String[] temp = data.split("#");
		    if(temp.length!=2) return;
		    context.write(new Text(temp[0]), new Text(temp[1]));
		  }
		}
		
		public static class TestReducer 
		     extends Reducer<Text,Text,Text,Text> {
		
		  public void reduce(Text key, Iterable<Text> values, 
		                     Context context
		                     ) throws IOException, InterruptedException {
		    PointwiseStart test = new PointwiseStart();
		    for(Text val : values){
		    	List<Double> x = new ArrayList<Double>();
		    	String[] temp = val.toString().split(":");
		    	if(temp.length!=2) return;
		    	String[] features = temp[1].split(",");
		    	for(String node : features){
		    		x.add(Double.parseDouble(node));
		    	}
		    	test.testX.add(x);
		    	test.testDocid.add(temp[0]);
		    }
		    Map<String,List<String>> result = test.BestModelF();
		    context.write(key,new Text(result.toString()));
		  }
		}
		
		public static void start(String inputpath,String outputpath,String lrmodel,String lirmodel) throws Exception {
			  Configuration conf = new Configuration();
//			  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//			  if (otherArgs.length != 4) {
//			    System.err.println("Usage: wordcount <in> <out>");
//			    System.exit(2);
//			  }
			  conf.set("lrmodel", lrmodel);
			  conf.set("lirmodel", lirmodel);
			  Job job = new Job(conf, "word count");
			  job.setJarByClass(modelTest.class);
			  job.setMapperClass(TestMapper.class);
			  job.setReducerClass(TestReducer.class);
			  job.setOutputKeyClass(Text.class);
			  job.setOutputValueClass(Text.class);
			  FileInputFormat.addInputPath(job, new Path(inputpath));
			  FileOutputFormat.setOutputPath(job, new Path(outputpath));
			  System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
		
		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
			  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			  if (otherArgs.length != 4) {
			    System.err.println("Usage: wordcount <in> <out>");
			    System.exit(2);
			  }
			  HdfsDAO hdfs = new HdfsDAO(conf);
			  String LIRModel = hdfs.cat(otherArgs[2]);
			  String LRModel = hdfs.cat(otherArgs[3]);
			  modelTest test = new modelTest();
			  test.start(otherArgs[0], otherArgs[1], LRModel, LIRModel);
		}

}

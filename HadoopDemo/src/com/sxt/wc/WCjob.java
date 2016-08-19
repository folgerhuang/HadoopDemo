package com.sxt.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCjob {
	
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://n1:8020");
			conf.set("yarn.resourcemanager.hostname", "n1");
			//conf.set("mapred.jar", "D:\\wc.jar");
			Job job = Job.getInstance(conf);
			job.setJobName("wc");
			job.setMapperClass(WCMapper.class);
			job.setReducerClass(WCReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPaths(job, "/user/shiyu/wc/input");
			Path path = new Path("/user/shiyu/out");
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(path))
			{
				fs.delete(path, true);
			}
			FileOutputFormat.setOutputPath(job, path);
			
			boolean waitForCompletion = job.waitForCompletion(true);
			System.out.println(waitForCompletion == true? (String) "Successed":(String) "Failed");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

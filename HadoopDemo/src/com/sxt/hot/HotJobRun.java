package com.sxt.hot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HotJobRun {
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME","root");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://n1:8020");
		conf.set("yarn.resourcemanager.hostname", "n1");
		Job job = new Job(conf);
		job.setJobName("hot sort");
		job.setJarByClass(HotJobRun.class);
		job.setMapperClass(HotMapper.class);
		job.setReducerClass(HotReducer.class);
		job.setMapOutputKeyClass(HotDomain.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(HotPartition.class);
		job.setSortComparatorClass(HotSort.class);
		job.setGroupingComparatorClass(HotGroup.class);
		job.setNumReduceTasks(3);

		FileInputFormat.setInputPaths(job, new Path("/user/shiyu/hot/input/"));
		FileSystem fs= FileSystem.get(conf);
		Path f = new Path("/user/shiyu/hot/output/");
		if(fs.exists(f))
		{
			fs.delete(f, true);
		}
		FileOutputFormat
				.setOutputPath(job, f);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

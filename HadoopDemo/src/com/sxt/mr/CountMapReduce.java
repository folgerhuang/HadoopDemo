package com.sxt.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jboss.netty.util.internal.StringUtil;


/**
 * 测试数据： 
 * 用户id 收入 支出 
 * 1 1000 0 
 * 2 500 300 
 * 1 2000 1000 
 * 2 500 200
 * 
 * 需求：
 *  用户id 总收入 总支出 总余额
 *  1 3000 1000 2000 
 *  2 1000 500 500
 * 
 * @author shiyu
 *
 */
public class CountMapReduce {

	public static class CountMapper extends
			Mapper<LongWritable, Text, IntWritable, UserWritable> {
		private UserWritable userWritable = new UserWritable();
		private IntWritable id = new IntWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 获取每行数据
			String line = value.toString();
			String[] words = StringUtil.split(line, '\t');
			if (words.length == 3) {
				userWritable
						.setId(Integer.parseInt(words[0]))
						.setIncome(Integer.parseInt(words[1]))
						.setExpense(Integer.parseInt(words[2]))
						.setSum(Integer.parseInt(words[1])
								- Integer.parseInt(words[2]));

				id.set(Integer.parseInt(words[0]));
			}

			context.write(id, userWritable);

		}
	}

	public static class CountReducer extends
			Reducer<IntWritable, UserWritable, UserWritable, NullWritable> {
		/**
		 * 输入数据： <1,{user01[1,1000,0,1000],user01[1,2000,1000,1000]}>
		 * <2,{user02[2,500,300,200],user02[2,500,200,300]}>
		 * 
		 * @author shiyu
		 *
		 */

		private UserWritable userWritable = new UserWritable();

		@Override
		protected void reduce(IntWritable key, Iterable<UserWritable> value,
				Context context) throws IOException, InterruptedException {
			int income = 0;
			int expense = 0;
			int sum = 0;
			for (UserWritable userWritable : value) {
				income += userWritable.getIncome();
				expense += userWritable.getExpense();
				sum += userWritable.getSum();
			}
			userWritable.setId(key.get()).setIncome(income).setExpense(expense)
					.setSum(sum);

			context.write(userWritable, NullWritable.get());

		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("HADOOP_USER_NAME", "root");
		
		//src下的配置文件
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://n2:8020");
		conf.set("yarn.resourcemanager.hostname", "n2");
		//conf.set("mapred.jar", "D:\\countmr.jar");
		Job job = Job.getInstance(conf,"countMr");
		job.setJarByClass(CountMapReduce.class);
		
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(UserWritable.class);
		
		job.setOutputKeyClass(UserWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		args = new String[]{
			"/user/shiyu/countmr/input",
			"/user/shiyu/countmr/output3"
		};
		FileSystem fs = FileSystem.get(conf);
		
		Path outPath = new Path(args[1]);
		if(fs.exists(outPath))
		{
			fs.delete(outPath,true);
		}
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean stats = job.waitForCompletion(true);
		if(!stats)
		{
			System.err.println("任务失败。。。 ");
		}
		
	}

}
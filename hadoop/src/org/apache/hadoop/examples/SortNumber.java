package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.HadoopUtil;

/**
* @ClassName: SortNumber
* @Description: 数据排序
* @author wgf
* @date 2015-1-14 上午11:06:02
* 
* 1）file1： 
	12
	22
	44
	111
	567
	1234
	7
	1 
	2）file2： 
	4567
	250
	10
	0 
	样例输出如下所示：
	1 0(序号 原始数据)
	2 1
	3 7
	...

*/ 
public class SortNumber {
	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			IntWritable data = new IntWritable();
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static IntWritable lineNumber = new IntWritable(1);
		@SuppressWarnings("unused")
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(lineNumber,key);
				lineNumber = new IntWritable(lineNumber.get()+1);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration(); // 系统参数

		Job job = Job.getInstance(conf, "SortNumber");
		job.setJarByClass(SortNumber.class);// 设置运行jar中的class名称
		job.setMapperClass(Map.class); // 设置mapreduce的mapper
		job.setReducerClass(Reduce.class);// 设置mapreduce的reducer
		job.setOutputKeyClass(IntWritable.class);// 设置输出key类型
		job.setOutputValueClass(IntWritable.class);// 设置输出value类型
		HadoopUtil.deleteDirectory("/output/sortNumber" ,job);//清理输出目录
		FileInputFormat.setInputPaths(job, new Path("/input/sortNumber"));// 设置文件输入目录
		FileOutputFormat.setOutputPath(job, new Path("/output/sortNumber"));// 设置文件输出目录
		if (!job.waitForCompletion(true))// 完成后退出
			return;
	}
}

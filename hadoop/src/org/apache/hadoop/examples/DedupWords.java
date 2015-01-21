package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.HadoopUtil;

/**
* @ClassName: Dedup
* @Description: 数据去重
* @author wgf
* @date 2015-1-13 上午10:02:02
* 
* 1）file1： 
	2012-3-1 a
	2012-3-2 b
	2012-3-3 c 
	2012-3-4 d 
	2012-3-5 a 
	2012-3-6 b
	2012-3-7 c
	2012-3-3 c 
	2）file2： 
	2012-3-1 b
	2012-3-2 a
	2012-3-3 b
	2012-3-4 d 
	2012-3-5 a 
	2012-3-6 c
	2012-3-7 d
	2012-3-3 c 
	样例输出如下所示：
	2012-3-1 a
	2012-3-1 b
	2012-3-2 a
	2012-3-2 b
	2012-3-3 b
	2012-3-3 c 
	2012-3-4 d 
	2012-3-5 a 
	2012-3-6 b
	2012-3-6 c
	2012-3-7 c
	2012-3-7 d

*/ 
public class DedupWords {

	// map--将输入的value直接复制到输出数据的key上并输出
	public static class Map extends Mapper<Object, Text, Text, Text> {
		//private static Text line = new Text(); // 每行数据

		// 实现map函数
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, new Text(""));
		}
	}

	// reduce 直接输出
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 实现reduce函数
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration(); // 系统参数

		Job job = Job.getInstance(conf, "DedupWords");
		job.setJarByClass(DedupWords.class);// 设置运行jar中的class名称
		job.setMapperClass(Map.class); // 设置mapreduce的mapper
		job.setReducerClass(Reduce.class);// 设置mapreduce的reducer
		job.setOutputKeyClass(Text.class);// 设置输出key类型
		job.setOutputValueClass(Text.class);// 设置输出value类型
		HadoopUtil.deleteDirectory("/output/dedupWords" ,job);//清理输出目录
		FileInputFormat.setInputPaths(job, new Path("/input/dedupWords"));// 设置文件输入目录
		FileOutputFormat.setOutputPath(job, new Path("/output/dedupWords"));// 设置文件输出目录
		if (!job.waitForCompletion(true))// 完成后退出
			return;
	}
}

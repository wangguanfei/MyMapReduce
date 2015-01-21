package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

public class WordCount {

	public static class WordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);

		private Text word = new Text();

		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer words = new StringTokenizer(line);
			while (words.hasMoreTokens()) {
				word.set(words.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable totalNum = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				sum += it.next().get();
			}
			totalNum.set(sum);
			context.write(key, totalNum);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(WordCount.class); // 设置运行jar中的class名称

		job.setMapperClass(WordCountMapper.class);// 设置mapreduce中的mapper reducer
													// combiner类
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class); // 设置输出结果键值对类型
		job.setOutputValueClass(IntWritable.class);
		HadoopUtil.deleteDirectory("/output/wordCount" ,job);//清理输出目录
		FileInputFormat.setInputPaths(job, new Path("/input/wordCount"));// 设置文件输入目录
		FileOutputFormat.setOutputPath(job, new Path("/output/wordCount"));// 设置文件输出目录

		if (!job.waitForCompletion(true))// 完成后退出
			return;
	}

}
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvgScore {
	private static Logger logger = LoggerFactory.getLogger(AvgScore.class);
	/**
	 * @Description: 平均分
	 * @author wgf
	 * @date 2015-1-14 下午4:29:05  
	 * @return void
	 * @throws
	 * 1）math： 
			张三	88
			李四	99
			王五	66
			赵六	77
		2）china： 
			张三	78
			李四	89
			王五	96
			赵六	67
		3）english： 
			张三	80
			李四	82
			王五	84
			赵六	86
		样本输出：
			张三 82
			李四 90
			王五 82
			赵六 76
	 */
	
	public static class ScoreMap extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			// 分割为行
			StringTokenizer line = new StringTokenizer(values.toString(), "\n");
			// 处理每行
			while (line.hasMoreTokens()) {
				StringTokenizer nameScore = new StringTokenizer(line.nextToken());
				String name = nameScore.nextToken();//姓名
				String score = nameScore.nextToken();//分数
				context.write( new Text(name), new IntWritable(Integer.parseInt(score)));
			}
			
		}
	}
	
	public static class ScoreReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int count = 0;
			int totalScore = 0;
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				totalScore += it.next().get();
				count++;
			}
			int avgScore = (int)totalScore/count;
			context.write(key, new IntWritable(avgScore));
		}
			
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration(); // 系统参数
		
		Job job = Job.getInstance(conf, "avgScore");
		job.setJarByClass(AvgScore.class);// 设置运行jar中的class名称
		job.setMapperClass(ScoreMap.class); // 设置mapreduce的mapper
		job.setReducerClass(ScoreReduce.class);// 设置mapreduce的reducer
		job.setOutputKeyClass(Text.class);// 设置输出key类型
		job.setOutputValueClass(IntWritable.class);// 设置输出value类型
		
		HadoopUtil.deleteDirectory("/output/avgScore" ,job);//清理输出目录
		
		FileInputFormat.setInputPaths(job, new Path("/input/avgScore"));// 设置文件输入目录
		FileOutputFormat.setOutputPath(job, new Path(new Path("/output/avgScore"),"result-2015"));// 设置文件输出目录
		if (!job.waitForCompletion(true))// 完成后退出
			logger.info("AvgScore  finished~~~~~~");
			return;
	}
}

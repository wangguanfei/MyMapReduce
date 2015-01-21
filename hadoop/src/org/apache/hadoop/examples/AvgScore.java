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
	 * @Description: ƽ����
	 * @author wgf
	 * @date 2015-1-14 ����4:29:05  
	 * @return void
	 * @throws
	 * 1��math�� 
			����	88
			����	99
			����	66
			����	77
		2��china�� 
			����	78
			����	89
			����	96
			����	67
		3��english�� 
			����	80
			����	82
			����	84
			����	86
		���������
			���� 82
			���� 90
			���� 82
			���� 76
	 */
	
	public static class ScoreMap extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			// �ָ�Ϊ��
			StringTokenizer line = new StringTokenizer(values.toString(), "\n");
			// ����ÿ��
			while (line.hasMoreTokens()) {
				StringTokenizer nameScore = new StringTokenizer(line.nextToken());
				String name = nameScore.nextToken();//����
				String score = nameScore.nextToken();//����
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

		Configuration conf = new Configuration(); // ϵͳ����
		
		Job job = Job.getInstance(conf, "avgScore");
		job.setJarByClass(AvgScore.class);// ��������jar�е�class����
		job.setMapperClass(ScoreMap.class); // ����mapreduce��mapper
		job.setReducerClass(ScoreReduce.class);// ����mapreduce��reducer
		job.setOutputKeyClass(Text.class);// �������key����
		job.setOutputValueClass(IntWritable.class);// �������value����
		
		HadoopUtil.deleteDirectory("/output/avgScore" ,job);//�������Ŀ¼
		
		FileInputFormat.setInputPaths(job, new Path("/input/avgScore"));// �����ļ�����Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path(new Path("/output/avgScore"),"result-2015"));// �����ļ����Ŀ¼
		if (!job.waitForCompletion(true))// ��ɺ��˳�
			logger.info("AvgScore  finished~~~~~~");
			return;
	}
}

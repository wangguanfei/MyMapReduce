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
* @Description: ����ȥ��
* @author wgf
* @date 2015-1-13 ����10:02:02
* 
* 1��file1�� 
	2012-3-1 a
	2012-3-2 b
	2012-3-3 c 
	2012-3-4 d 
	2012-3-5 a 
	2012-3-6 b
	2012-3-7 c
	2012-3-3 c 
	2��file2�� 
	2012-3-1 b
	2012-3-2 a
	2012-3-3 b
	2012-3-4 d 
	2012-3-5 a 
	2012-3-6 c
	2012-3-7 d
	2012-3-3 c 
	�������������ʾ��
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

	// map--�������valueֱ�Ӹ��Ƶ�������ݵ�key�ϲ����
	public static class Map extends Mapper<Object, Text, Text, Text> {
		//private static Text line = new Text(); // ÿ������

		// ʵ��map����
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, new Text(""));
		}
	}

	// reduce ֱ�����
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// ʵ��reduce����
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration(); // ϵͳ����

		Job job = Job.getInstance(conf, "DedupWords");
		job.setJarByClass(DedupWords.class);// ��������jar�е�class����
		job.setMapperClass(Map.class); // ����mapreduce��mapper
		job.setReducerClass(Reduce.class);// ����mapreduce��reducer
		job.setOutputKeyClass(Text.class);// �������key����
		job.setOutputValueClass(Text.class);// �������value����
		HadoopUtil.deleteDirectory("/output/dedupWords" ,job);//�������Ŀ¼
		FileInputFormat.setInputPaths(job, new Path("/input/dedupWords"));// �����ļ�����Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path("/output/dedupWords"));// �����ļ����Ŀ¼
		if (!job.waitForCompletion(true))// ��ɺ��˳�
			return;
	}
}

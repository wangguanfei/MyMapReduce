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
* @Description: ��������
* @author wgf
* @date 2015-1-14 ����11:06:02
* 
* 1��file1�� 
	12
	22
	44
	111
	567
	1234
	7
	1 
	2��file2�� 
	4567
	250
	10
	0 
	�������������ʾ��
	1 0(��� ԭʼ����)
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

		Configuration conf = new Configuration(); // ϵͳ����

		Job job = Job.getInstance(conf, "SortNumber");
		job.setJarByClass(SortNumber.class);// ��������jar�е�class����
		job.setMapperClass(Map.class); // ����mapreduce��mapper
		job.setReducerClass(Reduce.class);// ����mapreduce��reducer
		job.setOutputKeyClass(IntWritable.class);// �������key����
		job.setOutputValueClass(IntWritable.class);// �������value����
		HadoopUtil.deleteDirectory("/output/sortNumber" ,job);//�������Ŀ¼
		FileInputFormat.setInputPaths(job, new Path("/input/sortNumber"));// �����ļ�����Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path("/output/sortNumber"));// �����ļ����Ŀ¼
		if (!job.waitForCompletion(true))// ��ɺ��˳�
			return;
	}
}

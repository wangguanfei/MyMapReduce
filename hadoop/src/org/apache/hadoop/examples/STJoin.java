package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName: STJoin
 * @Description: 单表关联
 * @author wgf
 * @date 2015-1-16 上午10:19:45 
 * 
                输入
  child parent 
	Tom Lucy
	Tom Jack
	Jone Lucy
	Jone Jack
	Lucy Mary
	Lucy Ben
	Jack Alice
	Jack Jesse
	Terry Alice
	Terry Jesse
	Philip Terry
	Philip Alma
	Mark Terry
	Mark Alma
----------------------	
	输出
 * grandchild grandparent 
	Tom 　　 Alice
	Tom 　　 Jesse
	Jone 　　Alice
	Jone 　　Jesse
	Tom 　　 Mary
	Tom 　　 Ben
	Jone 　　Mary
	Jone 　　Ben
	Philip 　Alice
	Philip 　Jesse
	Mark 　　Alice
	Mark 　　Jesse

 */
public class STJoin {
	private static Logger logger = LoggerFactory.getLogger(STJoin.class);
	public static int time = 0;

	/*
	 * map将输出分割child和parent，然后正序输出一次作为右表， 反序输出一次作为左表，需要注意的是在输出的value中必须
	 * 加上左右表的区别标识。
	 */
	public static class Map extends Mapper<Object, Text, Text, Text> {
		// 实现map函数
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String childname = new String();// 孩子名称
			String parentname = new String();// 父母名称
			String relationtype = new String();// 左右表标识
			// 输入的一行预处理文本
			StringTokenizer itr = new StringTokenizer(value.toString());
			String[] values = new String[2];
			int i = 0;
			while (itr.hasMoreTokens()) {
				values[i] = itr.nextToken();
				i++;
			}
			if (values[0].compareTo("child") != 0) {
				childname = values[0];
				parentname = values[1];
				// 输出左表
				relationtype = "1";
				context.write(new Text(values[1]), new Text(relationtype + "+"
						+ childname + "+" + parentname));
				// 输出右表
				relationtype = "2";
				context.write(new Text(values[0]), new Text(relationtype + "+"
						+ childname + "+" + parentname));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 输出表头
			if (0 == time) {
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			int grandchildnum = 0;
			String[] grandchild = new String[10];
			int grandparentnum = 0;
			String[] grandparent = new String[10];
			Iterator<Text> ite = values.iterator();
			while (ite.hasNext()) {
				String record = ite.next().toString();
				int len = record.length();
				int i = 2;
				if (0 == len) {
					continue;
				}
				// 取得左右表标识
				char relationtype = record.charAt(0);
				// 定义孩子和父母变量
				String childname = new String();
				String parentname = new String();
				// 获取value-list中value的child
				while (record.charAt(i) != '+') {
					childname += record.charAt(i);
					i++;
				}
				i = i + 1;
				// 获取value-list中value的parent
				while (i < len) {
					parentname += record.charAt(i);
					i++;
				}
				// 左表，取出child放入grandchildren
				if ('1' == relationtype) {
					grandchild[grandchildnum] = childname;
					grandchildnum++;
				}
				// 右表，取出parent放入grandparent
				if ('2' == relationtype) {
					grandparent[grandparentnum] = parentname;
					grandparentnum++;
				}
			}
			// grandchild和grandparent数组求笛卡尔儿积
			if (0 != grandchildnum && 0 != grandparentnum) {
				for (int m = 0; m < grandchildnum; m++) {
					for (int n = 0; n < grandparentnum; n++) {
						// 输出结果
						context.write(new Text(grandchild[m]), new Text(
								grandparent[n]));
					}
				}
			}
		}
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); // 系统参数

		Job job = Job.getInstance(conf, "sTJoin");
		job.setJarByClass(STJoin.class);// 设置运行jar中的class名称
		job.setMapperClass(Map.class); // 设置mapreduce的mapper
		job.setReducerClass(Reduce.class);// 设置mapreduce的reducer
		job.setOutputKeyClass(Text.class);// 设置输出key类型
		job.setOutputValueClass(Text.class);// 设置输出value类型

		HadoopUtil.deleteDirectory("/output/sTJoin", job);// 清理输出目录

		FileInputFormat.setInputPaths(job, new Path("/input/sTJoin"));// 设置文件输入目录
		FileOutputFormat.setOutputPath(job, new Path("/output/sTJoin"));// 设置文件输出目录
		if (!job.waitForCompletion(true))// 完成后退出
			logger.info("sTJoin  finished~~~~~~");
		return;
	}

}
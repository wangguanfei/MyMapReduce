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
 * @Description: �������
 * @author wgf
 * @date 2015-1-16 ����10:19:45 
 * 
                ����
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
	���
 * grandchild grandparent 
	Tom ���� Alice
	Tom ���� Jesse
	Jone ����Alice
	Jone ����Jesse
	Tom ���� Mary
	Tom ���� Ben
	Jone ����Mary
	Jone ����Ben
	Philip ��Alice
	Philip ��Jesse
	Mark ����Alice
	Mark ����Jesse

 */
public class STJoin {
	private static Logger logger = LoggerFactory.getLogger(STJoin.class);
	public static int time = 0;

	/*
	 * map������ָ�child��parent��Ȼ���������һ����Ϊ�ұ� �������һ����Ϊ�����Ҫע������������value�б���
	 * �������ұ�������ʶ��
	 */
	public static class Map extends Mapper<Object, Text, Text, Text> {
		// ʵ��map����
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String childname = new String();// ��������
			String parentname = new String();// ��ĸ����
			String relationtype = new String();// ���ұ��ʶ
			// �����һ��Ԥ�����ı�
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
				// ������
				relationtype = "1";
				context.write(new Text(values[1]), new Text(relationtype + "+"
						+ childname + "+" + parentname));
				// ����ұ�
				relationtype = "2";
				context.write(new Text(values[0]), new Text(relationtype + "+"
						+ childname + "+" + parentname));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// ʵ��reduce����
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// �����ͷ
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
				// ȡ�����ұ��ʶ
				char relationtype = record.charAt(0);
				// ���庢�Ӻ͸�ĸ����
				String childname = new String();
				String parentname = new String();
				// ��ȡvalue-list��value��child
				while (record.charAt(i) != '+') {
					childname += record.charAt(i);
					i++;
				}
				i = i + 1;
				// ��ȡvalue-list��value��parent
				while (i < len) {
					parentname += record.charAt(i);
					i++;
				}
				// ���ȡ��child����grandchildren
				if ('1' == relationtype) {
					grandchild[grandchildnum] = childname;
					grandchildnum++;
				}
				// �ұ�ȡ��parent����grandparent
				if ('2' == relationtype) {
					grandparent[grandparentnum] = parentname;
					grandparentnum++;
				}
			}
			// grandchild��grandparent������ѿ�������
			if (0 != grandchildnum && 0 != grandparentnum) {
				for (int m = 0; m < grandchildnum; m++) {
					for (int n = 0; n < grandparentnum; n++) {
						// ������
						context.write(new Text(grandchild[m]), new Text(
								grandparent[n]));
					}
				}
			}
		}
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); // ϵͳ����

		Job job = Job.getInstance(conf, "sTJoin");
		job.setJarByClass(STJoin.class);// ��������jar�е�class����
		job.setMapperClass(Map.class); // ����mapreduce��mapper
		job.setReducerClass(Reduce.class);// ����mapreduce��reducer
		job.setOutputKeyClass(Text.class);// �������key����
		job.setOutputValueClass(Text.class);// �������value����

		HadoopUtil.deleteDirectory("/output/sTJoin", job);// �������Ŀ¼

		FileInputFormat.setInputPaths(job, new Path("/input/sTJoin"));// �����ļ�����Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path("/output/sTJoin"));// �����ļ����Ŀ¼
		if (!job.waitForCompletion(true))// ��ɺ��˳�
			logger.info("sTJoin  finished~~~~~~");
		return;
	}

}
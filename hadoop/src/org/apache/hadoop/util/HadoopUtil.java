package org.apache.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class HadoopUtil {

	public static void deleteDirectory(String directory, Job job)
			throws IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(new Path(directory))) {
			fs.delete(new Path(directory), true);
			System.out.println("存在此路径, 已经删除......");
		}
	}
}

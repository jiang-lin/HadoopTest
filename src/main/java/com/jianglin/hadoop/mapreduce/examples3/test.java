package com.jianglin.hadoop.mapreduce.examples3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class test {
	
	public static void main(String[] args) {
		
		try{
			Configuration conf =new Configuration(false);
			conf.addResource("hadoop-hdfs-site.xml");
			conf.addResource("hadoop-core-site.xml");
			Job job=new Job(conf);
			job.setJarByClass(test.class);
			job.setMapperClass(MapperClass.class);
			job.setReducerClass(ReduceClass.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
//			FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/paixu"));
//			FileOutputFormat.setOutputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/paixu/output"));
			FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples3/input/file1.txt"));
			FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples3/input/file2.txt"));
			FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples3/input/output"));

			System.exit(job.waitForCompletion(true)?0:1);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
	}

}

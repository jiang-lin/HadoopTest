package com.jianglin.hadoop.mapreduce.examples2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class test {
	/*1）file1：
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
    2012-3-7 d*/

	public static void main(String[] args) {
		try{
			Configuration conf=new Configuration(false);
			conf.addResource("hadoop-hdfs-site.xml");
			conf.addResource("hadoop-core-site.xml");
			
			Job job=new Job(conf);
			job.setJarByClass(test.class);
			job.setMapperClass(MapDedup.class);
			job.setReducerClass(ReduceDudup.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			//FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/dedup"));
//			FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/dedup/file1.txt"));
//			FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/dedup/file2.txt"));
//			FileOutputFormat.setOutputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/dedup/output1"));
			FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples2/input/file1.txt"));
			FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples2/input/file2.txt"));
			FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples2/input/output"));


			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}

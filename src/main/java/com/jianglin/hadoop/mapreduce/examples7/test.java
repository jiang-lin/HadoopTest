package com.jianglin.hadoop.mapreduce.examples7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
			job.setMapOutputKeyClass(JoinPaire.class);
			job.setMapOutputValueClass(Text.class);
//			job.setPartitionerClass(PartitionByText.class);//自定义分区
			//job.setSortComparatorClass(TextComparator.class);
			job.setSortComparatorClass(TextIntComparator.class);
			job.setGroupingComparatorClass(TextComparator.class);
			job.setReducerClass(ReduceClass.class);
//			FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/join2/file2.txt"));
//			FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/join2/file1.txt"));
//
//			FileOutputFormat.setOutputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/join2/output"));
			FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples7/input"));
			FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples7/input/output"));
			System.exit(job.waitForCompletion(true)?0:1);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
	}

}

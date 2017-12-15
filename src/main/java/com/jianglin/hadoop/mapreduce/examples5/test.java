package com.jianglin.hadoop.mapreduce.examples5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class test {

	public static void main(String[] args) {

		try{
			Configuration conf =new Configuration(false);
			conf.addResource("hadoop-hdfs-site.xml");
			conf.addResource("hadoop-core-site.xml");
			Job job=new Job(conf);
			job.setJarByClass(test.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setMapperClass(MapperClass.class);
			job.setReducerClass(ReduceClass.class);
			job.setMapOutputKeyClass(SortPaire.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setSortComparatorClass(TextIntComparator.class);
			job.setGroupingComparatorClass(TextComparator.class);

			job.setPartitionerClass(PartitionByText.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

//			FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/sort/file.txt"));
//			FileOutputFormat.setOutputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/sort/output"));
			FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples5/input"));
			FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples5/input/output"));
			System.exit(job.waitForCompletion(true)?0:1);
		}catch(Exception e){
			e.printStackTrace();
		}


	}

}

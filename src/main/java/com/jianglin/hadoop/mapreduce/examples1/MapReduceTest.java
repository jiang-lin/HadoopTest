package com.jianglin.hadoop.mapreduce.examples1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MapReduceTest {
	
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration(false);
	    conf.addResource("hadoop-hdfs-site.xml");
		conf.addResource("hadoop-core-site.xml");
		/*String [] otherArgs=new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length!=2){
			System.out.println("111111");
			System.exit(2);
		}*/
		Job job=new Job(conf);
		job.setJarByClass(MapReduceTest.class);
		job.setMapperClass(MapperClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/WordCount.txt"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/output"));FileInputFormat.addInputPath(job, new Path("hdfs://hmaster.vargo.com.cn:8020/mapReduce/testFile/WordCount.txt"));
		FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples1/input/WordCount.txt"));
		FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples1/input/output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

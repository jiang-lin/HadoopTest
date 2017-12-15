package com.jianglin.hadoop.mapreduce.examples8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by admin on 2017/10/25.
 */
public class mapReduceTest extends Configured implements Tool{

    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "jointest");
        //        job.setCombinerClass();
        job.setJarByClass(mapReduceTest.class);
        job.setMapperClass(MapperLeftOuterJoin.class);
        job.setReducerClass(ReduceLeftOuterJoin.class);

        job.setInputFormatClass(TextInputFormat.class);//设置文件输入格式
        job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格格式
        //设置map的输出key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CombineValues.class);
        //设置reduce的输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples8/input"));
        FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples8/input/output"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static  void main(String[] args){
        try {
            int ret=ToolRunner.run(new mapReduceTest(),args);
            System.exit(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

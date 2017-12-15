package com.jianglin.hadoop.mapreduce.examples9;

import com.jianglin.hadoop.mapreduce.examples9.MapperSideJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by admin on 2017/10/24.
 * 1、在Reudce端进行连接。
 * 在Reudce端进行连接是MapReduce框架进行表之间join操作最为常见的模式，其具体的实现原理如下：
 * Map端的主要工作：为来自不同表（文件）的key/value对打标签以区别不同来源的记录。然后用连接字段作为key，
 * 其余部分和新加的标志作为value，最后进行输出。
 * reduce端的主要工作：在reduce端以连接字段作为key的分组已经完成，
 * 我们只需要在每一个分组当中将那些来源于不同文件的记录（在map阶段已经打标志）分开，最后进行笛卡尔只就ok了。原理非常简单，下面来看一个实
 */
public class MapperSideTest {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            //为该job添加缓存文件
            DistributedCache.addCacheFile(new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples9/input/tb_dim_city.txt").toUri(), configuration);
            Job job = new Job(configuration, "jointest");
            job.setNumReduceTasks(0);
            //        job.setCombinerClass();

            job.setJarByClass(MapperSideTest.class);
            job.setMapperClass(MapperSideJoin.class);
//            job.setReducerClass(ReduceLeftOuterJoin.class);

            job.setInputFormatClass(TextInputFormat.class);//设置文件输入格式
            job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格格式
            //设置map的输出key和value类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置reduce的输出key和value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples9/input"));
            FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/examples9/input/output"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

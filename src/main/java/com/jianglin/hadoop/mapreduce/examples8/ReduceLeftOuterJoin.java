package com.jianglin.hadoop.mapreduce.examples8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2017/10/24.
 */
public class ReduceLeftOuterJoin extends Reducer<Text,CombineValues,Text,Text> {
    private static final Logger logger = LoggerFactory.getLogger(MapperLeftOuterJoin.class);

    @Override
    protected void reduce(Text key, Iterable<CombineValues> values, Context context) throws IOException, InterruptedException {
        List<String> users =new ArrayList<String>();
        List<String> userCitys =new ArrayList<String>();
       for (CombineValues combineValues:values){
           logger.info("combineValues:"+combineValues.toString());
            if(combineValues.getFlag().toString().equals("0")){
                userCitys.add(combineValues.getSecondPart().toString());
            }else if(combineValues.getFlag().toString().equals("1")){
                users.add(combineValues.getSecondPart().toString());
            }

       }
//       logger.info("values"+values.toString());
       logger.info("tb_dim_city:"+userCitys.toString());
       logger.info("tb_user_profiles:"+users.toString());
       Text value=new Text();
        for (String userCity:userCitys){
            for (String user:users){
//               if(user.getJoinKey().equals(userCity.getJoinKey())){
                   value.set(userCity+"\t"+user);
                   context.write(key,value);
//               }

           }
       }
    }

  /*  @Override
    public void run(Context context) throws IOException, InterruptedException {
        try {
            Configuration configuration = new Configuration();
            Job job = new Job(configuration, "jointest");
            //        job.setCombinerClass();
            job.setJarByClass(JoinMapReduceTest.class);
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
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

}

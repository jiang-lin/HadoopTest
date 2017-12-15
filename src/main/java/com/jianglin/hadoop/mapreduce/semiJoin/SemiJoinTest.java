package com.jianglin.hadoop.mapreduce.semiJoin;

import com.jianglin.hadoop.mapreduce.examples8.CombineValues;
import com.jianglin.hadoop.mapreduce.examples8.JoinMapReduceTest;
import com.jianglin.hadoop.mapreduce.examples8.MapperLeftOuterJoin;
import com.jianglin.hadoop.mapreduce.examples8.ReduceLeftOuterJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
 * Created by admin on 2017/10/24.
 **
 * 用途说明：
 * reudce side join中的left outer join
 * 左连接，两个文件分别代表2个表,连接字段table1的id字段和table2的cityID字段
 * table1(左表):tb_dim_city(id int,name string,orderid int,city_code,is_show)
 * tb_dim_city.dat文件内容,分隔符为"|"：
 * id     name  orderid  city_code  is_show
 * 0       其他        9999     9999         0
 * 1       长春        1        901          1
 * 2       吉林        2        902          1
 * 3       四平        3        903          1
 * 4       松原        4        904          1
 * 5       通化        5        905          1
 * 6       辽源        6        906          1
 * 7       白城        7        907          1
 * 8       白山        8        908          1
 * 9       延吉        9        909          1
 * -------------------------风骚的分割线-------------------------------
 * table2(右表)：tb_user_profiles(userID int,userName string,network string,double flow,cityID int)
 * tb_user_profiles.dat文件内容,分隔符为"|"：
 * userID   network     flow    cityID
 * 1           2G       123      1
 * 2           3G       333      2
 * 3           3G       555      1
 * 4           2G       777      3
 * 5           3G       666      4
 * -------------------------风骚的分割线-------------------------------
 * joinKey.dat内容：
 * city_code
 * 1
 * 2
 * 3
 * 4
 * -------------------------风骚的分割线-------------------------------
 *  结果：
 *  1   长春  1   901 1   1   2G  123
 *  1   长春  1   901 1   3   3G  555
 *  2   吉林  2   902 1   2   3G  333
 *  3   四平  3   903 1   4   2G  777
 *  4   松原  4   904 1   5   3G  666
 */
public class SemiJoinTest extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int ret= ToolRunner.run(new SemiJoinTest(),args);
        System.exit(ret);
    }

    public int run(String[] strings) throws Exception {
        try {
            Configuration configuration = new Configuration();
            //为该job添加缓存文件
            DistributedCache.addCacheFile(new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/semiJoin/input/joinKey.txt").toUri(), configuration);
            Job job = new Job(configuration, "jointest");
            //        job.setCombinerClass();
            job.setJarByClass(SemiJoinTest.class);
            job.setMapperClass(SemiJoinMapper.class);
            job.setReducerClass(ReduceLeftOuterJoin.class);

            job.setInputFormatClass(TextInputFormat.class);//设置文件输入格式
            job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格格式
            //设置map的输出key和value类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CombineValues.class);
            //设置reduce的输出key和value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/semiJoin/input"));
            FileOutputFormat.setOutputPath(job, new Path("E:/JiangLin/project_git/hadoopTest/src/main/java/com/jianglin/hadoop/mapreduce/semiJoin/input/output"));

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}

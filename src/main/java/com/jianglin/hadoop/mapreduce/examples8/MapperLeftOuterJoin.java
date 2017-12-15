package com.jianglin.hadoop.mapreduce.examples8;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author jianglin
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
 *
 * -------------------------风骚的分割线-------------------------------
 *  结果：
 *  1   长春  1   901 1   1   2G  123
 *  1   长春  1   901 1   3   3G  555
 *  2   吉林  2   902 1   2   3G  333
 *  3   四平  3   903 1   4   2G  777
 *  4   松原  4   904 1   5   3G  666
 */
public class MapperLeftOuterJoin extends Mapper<Object,Text,Text,CombineValues> {
    private static final Logger logger = LoggerFactory.getLogger(MapperLeftOuterJoin.class);
    enum Counter{
        LINKSIP
    }
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try{
            if (value.toString().contains("id") == true || value.toString().contains("userID") == true) {
                return;
            }

            Text joinKey=new Text();
            Text flag=new Text();
            Text secondPart=new Text();
            String filepath=((FileSplit) context.getInputSplit()).getPath().toString();
            System.out.println("filepath:"+filepath);
            CombineValues combineValues = new CombineValues();
            if(filepath.endsWith("tb_user_profiles.txt")){
                if(value==null){
                    return;
                }
                String[] valueItems = value.toString().split("\t");
                if(valueItems.length!=4){
                    return;
                }
                flag.set("1");
                joinKey.set(valueItems[3]);
                secondPart.set(valueItems[0]+"\t"+valueItems[1]+"\t"+valueItems[2]);
                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(joinKey,combineValues);

            }else if (filepath.endsWith("tb_dim_city.txt")){
                if(value==null){
                    return;
                }
                String[] valueItems = value.toString().split("\t");
                if(valueItems.length!=5){
                    return;
                }

                flag.set("0");
                joinKey.set(valueItems[0]);
                secondPart.set(valueItems[1]+"\t"+valueItems[2]+"\t"+valueItems[3]+"\t"+valueItems[4]);
                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(joinKey,combineValues);
            }
            logger.info("::::"+combineValues.toString());
        }catch (Exception e){
            context.getCounter(Counter.LINKSIP).increment(1);
        }



//        if(filepath.endsWith("tb_dim_city.dat")){
//            String[] valueItems = value.toString().split("\\|");
//            //过滤格式错误的记录
//            if(valueItems.length != 5){
//                return;
//            }
//            flag.set("0");
//            joinKey.set(valueItems[0]);
//            secondPart.set(valueItems[1]+"\t"+valueItems[2]+"\t"+valueItems[3]+"\t"+valueItems[4]);
//            combineValues.setFlag(flag);
//            combineValues.setJoinKey(joinKey);
//            combineValues.setSecondPart(secondPart);
//            context.write(combineValues.getJoinKey(), combineValues);
//
//        }//数据来自于tb_user_profiles.dat，标志即为"1"
//        else if(filepath.endsWith("tb_user_profiles.dat")){
//            String[] valueItems = value.toString().split("\\|");
//            //过滤格式错误的记录
//            if(valueItems.length != 4){
//                return;
//            }
//            flag.set("1");
//            joinKey.set(valueItems[3]);
//            secondPart.set(valueItems[0]+"\t"+valueItems[1]+"\t"+valueItems[2]);
//            combineValues.setFlag(flag);
//            combineValues.setJoinKey(joinKey);
//            combineValues.setSecondPart(secondPart);
//            context.write(combineValues.getJoinKey(), combineValues);
//        }
    }
}

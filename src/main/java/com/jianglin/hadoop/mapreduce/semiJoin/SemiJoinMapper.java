package com.jianglin.hadoop.mapreduce.semiJoin;

import com.jianglin.hadoop.mapreduce.examples8.CombineValues;
import com.jianglin.hadoop.mapreduce.examples8.MapperLeftOuterJoin;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

/**
 * Created by admin on 2017/10/24.
 */
public class SemiJoinMapper extends Mapper<Object,Text,Text,CombineValues> {
    private static final Logger logger = LoggerFactory.getLogger(SemiJoinMapper.class);
    private HashSet<String> joinKeySet = new HashSet<String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = null;
        //获得当前作业的DistributedCache相关文件

//        Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        URI[] distributePaths = DistributedCache.getCacheFiles(context.getConfiguration());
        String joinKeyStr = null;
        for(URI p : distributePaths){
            if(p.toString().endsWith("joinKey.txt")){
                br = new BufferedReader(new FileReader(p.toString()));
                while(null!=(joinKeyStr=br.readLine())){
                    if (joinKeyStr.toString().contains("cityID") == true) {
                        continue;
                    }
                    joinKeySet.add(joinKeyStr);
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
            if(joinKeySet.contains(valueItems[3])){
                flag.set("1");
                joinKey.set(valueItems[3]);
                secondPart.set(valueItems[0]+"\t"+valueItems[1]+"\t"+valueItems[2]);
                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(joinKey,combineValues);
            }

        }else if (filepath.endsWith("tb_dim_city.txt")){
            if(value==null){
                return;
            }
            String[] valueItems = value.toString().split("\t");
            if(valueItems.length!=5){
                return;
            }
            if(joinKeySet.contains(valueItems[0])){
                flag.set("0");
                joinKey.set(valueItems[0]);
                secondPart.set(valueItems[1]+"\t"+valueItems[2]+"\t"+valueItems[3]+"\t"+valueItems[4]);
                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(joinKey,combineValues);
            }
        }
        logger.info("::::"+combineValues.toString());
    }
}

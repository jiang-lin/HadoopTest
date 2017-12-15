package com.jianglin.hadoop.mapreduce.examples9;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

/**
 * Created by admin on 2017/10/24.
 */
public class MapperSideJoin extends Mapper<Object,Text,Text,Text>{
    private Text outPutKey = new Text();
    private Text outPutValue = new Text();
    private String mapInputStr = null;
    private String mapInputSpit[] = null;
    private String city_secondPart = null;
    private HashMap<String,String> city_info = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = null;
        //获得当前作业的DistributedCache相关文件

//        Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        URI[] distributePaths = DistributedCache.getCacheFiles(context.getConfiguration());
        String cityInfo = null;
        for(URI p : distributePaths){
            if(p.toString().endsWith("tb_dim_city.txt")){
                br = new BufferedReader(new FileReader(p.toString()));
                while(null!=(cityInfo=br.readLine())){
                    if (cityInfo.toString().contains("id") == true || cityInfo.toString().contains("userID") == true) {
                       continue;
                    }
                    String[] cityPart = cityInfo.split("\t",5);
                    if(cityPart.length ==5) {
                        city_info.put(cityPart[0], cityPart[1] + "\t" + cityPart[2] + "\t" + cityPart[3] + "\t" + cityPart[4]);
                    }
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(value==null){
            return;
        }
        mapInputStr = value.toString();
        mapInputSpit = mapInputStr.split("\t",4);
        //过滤非法记录
        if(mapInputSpit.length != 4){
            return;
        }
        //判断链接字段是否在map中存在
        city_secondPart = city_info.get(mapInputSpit[3]);
        if(city_secondPart != null){
            this.outPutKey.set(mapInputSpit[3]);
            this.outPutValue.set(city_secondPart+"\t"+mapInputSpit[0]+"\t"+mapInputSpit[1]+"\t"+mapInputSpit[2]);
            context.write(outPutKey, outPutValue);
        }
    }
}

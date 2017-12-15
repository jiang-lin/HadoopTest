package com.jianglin.hadoop.mapreduce.examples6;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, Text>{

	private Text nameKey=new Text();
	private Text numValue=new Text();
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); 
	    String relationtype = new String();// 左右表标识
        // 输入文件首行，不处理
        if (line.contains("factoryname") == true || line.contains("addressID") == true) {
             return;
        } 
        StringTokenizer stringTokenizer=new StringTokenizer(line,"\"");
        while(stringTokenizer.hasMoreElements()){
        	String factoryname=stringTokenizer.nextToken();
        	String addressed=stringTokenizer.nextToken();
        	if(factoryname!=null && !"".equals(factoryname)&&addressed!=null && !"".equals(addressed)){
        		
        		if(factoryname.charAt(0)>='0' && factoryname.charAt(0)<='9'){
        			relationtype = "1";
        			nameKey.set(factoryname);
        			numValue.set(addressed+","+relationtype);
        		}else{
        			relationtype = "0";
        			nameKey.set(addressed);
        			numValue.set(factoryname+","+relationtype);
        		}
        	}
        	context.write(nameKey, numValue);
        }
        
	}
	
	public static void main(String[] args) {
		StringTokenizer stringTokenizer=new StringTokenizer("Shenzhen Thunder\"3","\"");
        while(stringTokenizer.hasMoreElements()){
        	
        	String name=stringTokenizer.nextToken();
        	String score=stringTokenizer.nextToken();
        	if(name!=null&&!"".equals(name))
        		name="";
        	if(score!=null&&!"".equals(score))
        		score="0";
        }
	}
}

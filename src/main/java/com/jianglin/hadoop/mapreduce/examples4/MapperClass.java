package com.jianglin.hadoop.mapreduce.examples4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, DoubleWritable>{

	private Text nameKey=new Text();
	private DoubleWritable numValue=new DoubleWritable(0);
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// 将输入的纯文本文件的数据转化成String
        String line = value.toString(); 
        StringTokenizer stringTokenizer=new StringTokenizer(line);
        while(stringTokenizer.hasMoreElements()){
        	
        	String name=stringTokenizer.nextToken();
        	String score=stringTokenizer.nextToken();
        	if(name==null || "".equals(name))
        		name="";
        	if(score==null||"".equals(score))
        		score="0";
        	nameKey.set(name);
        	numValue.set(Double.valueOf(score));
        	context.write(nameKey, numValue);
        }
        
	}
	
	public static void main(String[] args) {
		StringTokenizer stringTokenizer=new StringTokenizer("张三    78");
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

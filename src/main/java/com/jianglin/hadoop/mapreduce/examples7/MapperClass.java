package com.jianglin.hadoop.mapreduce.examples7;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperClass extends Mapper<Object, Text, JoinPaire, Text>{

	private Text numValue=new Text();
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); 
        FileSplit split=(FileSplit) context.getInputSplit();
        String filepath = "";
        if(split==null){
        	return ;
        }
        filepath = split.getPath().toString();
        StringTokenizer stringTokenizer=new StringTokenizer(line,"\"");
        System.out.println("filepath:"+filepath);
        JoinPaire nameKey=new JoinPaire();
        while(stringTokenizer.hasMoreElements()){
        	String strValue=stringTokenizer.nextToken();
        	String ValueId=stringTokenizer.nextToken();
        	numValue.set(ValueId);
        	
        	nameKey.setValue(strValue);
        	
        	if(filepath.indexOf("file1.txt")>=0){
        		nameKey.setAddressId(0);
        	}else if(filepath.indexOf("file2.txt")>=0){
        		nameKey.setAddressId(1);
        	}
        	System.out.println(nameKey.getValue()+","+nameKey.getAddressId());
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

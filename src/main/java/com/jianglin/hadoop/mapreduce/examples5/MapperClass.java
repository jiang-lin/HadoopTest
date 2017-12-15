package com.jianglin.hadoop.mapreduce.examples5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, SortPaire, IntWritable>{

	private SortPaire paire=new SortPaire(); 
	private IntWritable intvalue=new IntWritable();
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// 将输入的纯文本文件的数据转化成String
		int num=0;
		if(key!=null && value!=null){
			String line = value.toString();
			if(line!=null && !"".equals(line) && key!=null && !"".equals(key.toString()) ){
				num=Integer.valueOf(line);
				intvalue.set(num);
				paire.setName(key.toString());
				paire.setValue(num);
				context.write(paire,intvalue);
			}else{
				System.out.println(key+","+value);
			}
		}else{
			System.out.println(key+","+value);
		}
		
	}
}

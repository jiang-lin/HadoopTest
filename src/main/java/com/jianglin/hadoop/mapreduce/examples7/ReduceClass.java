package com.jianglin.hadoop.mapreduce.examples7;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceClass extends Reducer<JoinPaire, Text, Text, Text>{
	private Text reduceKey = new Text();
	@Override
	protected void reduce(JoinPaire key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		int i = 0;
		System.out.println(key.getValue()+",,,,"+key.getAddressId());
		for(Text text:values){
			System.out.println(text.toString());
			//System.out.println("keyi:"+i+"   "+text.toString());
			if(i==0){
				reduceKey.set(text);
			}else{
				//System.out.println("reduceKey:"+reduceKey.toString());
				context.write(reduceKey, text);
			}
			i+=1;
		}
	}
	
}

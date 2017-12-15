package com.jianglin.hadoop.mapreduce.examples6;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceClass extends Reducer<Text, Text, Text, Text>{
	private int time = 0;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		List<String> factorynames=new ArrayList<String>();
		List<String> addressnames=new ArrayList<String>();
		 if (0 == time) {
             context.write(new Text("factoryname"), new Text("		"+"addressname"));
             time++;

         } 
		String relationtype = "";
		String textStr= "";
		for(Text text:values){
			String str=text.toString();
			if(str!=null && !"".equals(str)){
				String[] arr=str.split(",");
				if(arr.length>=2){
					textStr = arr[0];
					relationtype = arr[1];
				}
			}
			if("1".equals(relationtype) && !"".equals(textStr)){
				addressnames.add(textStr);
			}else if("0".equals(relationtype) && !"".equals(textStr)){
				factorynames.add(textStr);
			}
		}
		
		for(String factoryname:factorynames){
			for(String addressname:addressnames){
				 context.write(new Text(factoryname), new Text("		"+addressname));
				 System.out.println("factoryname:"+factoryname+",addressname:"+addressname);
			}
		}
		
		
	}
	
}

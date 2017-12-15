package com.jianglin.hadoop.mapreduce.examples5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;




public class SortPaire implements WritableComparable<SortPaire> {

	private String name;
	private int value;
	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
		out.writeInt(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		name = in.readUTF();
		value = in.readInt();
	}

	@Override
	public int compareTo(SortPaire o) {
		return o.getName().compareTo(this.name);
	}

}

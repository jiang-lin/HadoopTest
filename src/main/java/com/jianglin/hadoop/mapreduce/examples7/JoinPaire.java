package com.jianglin.hadoop.mapreduce.examples7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class JoinPaire implements WritableComparable<JoinPaire>{
	
	private String value;
	
	private int addressId;
	
	
	public int getAddressId() {
		return addressId;
	}

	public void setAddressId(int addressId) {
		this.addressId = addressId;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(value);
		out.writeLong(addressId);
		
	}

	public void readFields(DataInput in) throws IOException {
		value =in.readUTF();
		addressId =in.readInt();
		
	}

	public int compareTo(JoinPaire o) {
//		if(o.getAddressId()>this.addressId){
//			return 1;
//		}else if(o.getAddressId()>this.addressId){
//			return 0;
//		}else{
//			return -1;
//		}
//		return o.getValue().compareTo(this.value);
//		if(!o.getValue().equals(this.value)){
//			return o.getAddressId()-this.addressId;
//		}
		//return o.getValue().compareTo(this.value);
		
//		if(o.getValue().equals(this.value)){
//			if(o.getAddressId()>this.getAddressId()){
//				return -1;
//			}else if(o.getAddressId()==this.getAddressId()){
//				return 0;
//			}else {
//				return 1;
//			}
//		}
		return this.value.compareTo(o.getValue());
		
	}

}

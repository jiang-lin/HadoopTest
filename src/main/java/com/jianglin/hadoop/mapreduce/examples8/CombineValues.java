package com.jianglin.hadoop.mapreduce.examples8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by admin on 2017/10/23.
 */
public class CombineValues implements WritableComparable<CombineValues>{
    private Text joinKey= new Text();//链接关键字
    private Text flag= new Text();//文件来源标志
    private Text secondPart= new Text();//除了链接键外的其他部分

    public int compareTo(CombineValues o) {
        return this.joinKey.compareTo(o.getJoinKey());
    }

    public void write(DataOutput out) throws IOException {
        this.joinKey.write(out);
        this.flag.write(out);
        this.secondPart.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.joinKey.readFields(in);
        this.flag.readFields(in);
        this.secondPart.readFields(in);
    }

    @Override
    public String toString() {
        return "[flag="+this.flag.toString()+",joinKey="+this.joinKey.toString()+",secondPart="+this.secondPart.toString()+"]";
    }

    public Text getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(Text joinKey) {
        this.joinKey = joinKey;
    }

    public Text getFlag() {
        return flag;
    }

    public void setFlag(Text flag) {
        this.flag = flag;
    }

    public Text getSecondPart() {
        return secondPart;
    }

    public void setSecondPart(Text secondPart) {
        this.secondPart = secondPart;
    }
}

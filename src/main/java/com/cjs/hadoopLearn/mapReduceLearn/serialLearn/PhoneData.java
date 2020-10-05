package com.cjs.hadoopLearn.mapReduceLearn.serialLearn;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneData implements Writable {
    private long upData;
    private long downData;

    //一定要有无参构造方法
    public PhoneData() {
        super();
    }

    public PhoneData(String phone, long upData, long downData) {
        this.upData = upData;
        this.downData = downData;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upData);
        dataOutput.writeLong(downData);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upData = dataInput.readLong();
        this.downData = dataInput.readLong();

    }

    public long getUpData() {
        return upData;
    }

    public long getDownData() {
        return downData;
    }

    public void setUpData(long upData) {
        this.upData = upData;
    }

    public void setDownData(long downData) {
        this.downData = downData;
    }

    @Override
    public String toString() {
        return "PhoneData{" +
                "upData=" + upData +
                ", downData=" + downData +
                '}';
    }
}

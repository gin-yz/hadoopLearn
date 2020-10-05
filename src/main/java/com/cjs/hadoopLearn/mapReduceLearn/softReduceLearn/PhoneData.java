/*
* hadoop默认对ｋｅｙ排序，需要在传给ｍａｐ输出的时候将要排序的ｂｅａｎ设置成ｋｅｙ
* */

package com.cjs.hadoopLearn.mapReduceLearn.softReduceLearn;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneData implements WritableComparable<PhoneData> {
    private Long upLoadData;
    private Long downLoadData;

    public PhoneData() {
        super();
    }

    @Override
    public int compareTo(PhoneData o) {
        return Long.compare(upLoadData + downLoadData, o.getDownLoadData() + o.getUpLoadData());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(upLoadData);
        dataOutput.writeLong(downLoadData);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upLoadData = dataInput.readLong();
        this.downLoadData = dataInput.readLong();
    }

    public void setUpLoadData(Long upLoadData) {
        this.upLoadData = upLoadData;
    }

    public void setDownLoadData(Long downLoadData) {
        this.downLoadData = downLoadData;
    }

    public Long getUpLoadData() {
        return upLoadData;
    }

    public Long getDownLoadData() {
        return downLoadData;
    }

    @Override
    public String toString() {
        return "upLoadData=" + upLoadData +
                ", downLoadData=" + downLoadData + " sum Data:" + String.valueOf(upLoadData + downLoadData);
    }
}

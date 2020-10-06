package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.partitionerLearn;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneData implements Writable {
    private Long upLoadData;
    private Long downLoadData;


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
                ", downLoadData=" + downLoadData + "sum Data:" + String.valueOf(upLoadData + downLoadData);
    }
}

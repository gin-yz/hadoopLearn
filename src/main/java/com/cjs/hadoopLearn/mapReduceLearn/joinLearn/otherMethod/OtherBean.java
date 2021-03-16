package com.cjs.hadoopLearn.mapReduceLearn.joinLearn.otherMethod;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OtherBean implements Writable {
    private int pid;
    private int id;
    private int amount;
    private String pname;


    public OtherBean() { }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(pid);
        dataOutput.writeInt(id);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pid = dataInput.readInt();
        this.id = dataInput.readInt();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

}

package com.cjs.hadoopLearn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.hadoop.io.WritableComparator.readVInt;


public class Test2 {
    public static void main(String[] args) throws IOException {

        Text.Comparator comparator = new Text.Comparator();
        Text text1 = new Text("abcd");
        Text text2 = new Text("abc");

        byte[] serialize1 = serialize(text1);
        byte[] serialize2 = serialize(text2);

        int firstL1 = WritableUtils.decodeVIntSize(serialize1[0]) + readVInt(serialize1, 0);
        int firstL2 = WritableUtils.decodeVIntSize(serialize2[0]) + readVInt(serialize2, 0);

        int cmp = comparator.compare(serialize1, 0, firstL1, serialize2, 0, firstL2);

    }


    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        writable.write(dataOutputStream);
        dataOutputStream.close();
        return outputStream.toByteArray();
    }
}

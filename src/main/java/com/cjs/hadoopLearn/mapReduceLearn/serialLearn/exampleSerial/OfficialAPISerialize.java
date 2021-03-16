package com.cjs.hadoopLearn.mapReduceLearn.serialLearn.exampleSerial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.serializer.*;

import java.io.IOException;
import java.util.Arrays;

public class OfficialAPISerialize {
    public static void main(String[] args) throws IOException {

        Text text = new Text("cjs");

        WritableSerialization writableSerialization = new WritableSerialization();

        Serializer<Writable> serializer = writableSerialization.getSerializer(Writable.class);
        Deserializer<Writable> deserializer = writableSerialization.getDeserializer(Writable.class);

        DataOutputBuffer out = new DataOutputBuffer();
        DataInputBuffer in = new DataInputBuffer();
        serializer.open(out);
        serializer.serialize(text);
        serializer.close();

        //将out中数据传给in
        in.reset(out.getData(),out.getLength());
        System.out.println(Arrays.toString(in.getData()));

        Text text1 = new Text();
        deserializer.open(in);
        deserializer.deserialize(text1);
        System.out.println(text1.toString());

        //或使用工厂方法,推荐
        DataOutputBuffer out1 = new DataOutputBuffer();
        DataInputBuffer in1 = new DataInputBuffer();
        SerializationFactory serializationFactory = new SerializationFactory(new Configuration());

        Serializer<Text> serializer1 = serializationFactory.getSerializer(Text.class);
        Deserializer<Text> deserializer1 = serializationFactory.getDeserializer(Text.class);

        serializer1.open(out1);
        serializer1.serialize(text);
        in1.reset(out1.getData(),out1.getLength());

        Text text2 = new Text();
        deserializer1.open(in1);
        deserializer1.deserialize(text2);

        System.out.println(text2.toString());

    }



}

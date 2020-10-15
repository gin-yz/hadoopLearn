/*
* 手动实现hadoop中内置实例类型序列化
* 及其优雅，必须看
* */

package com.cjs.hadoopLearn.mapReduceLearn.serialLearn.exampleSerial;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public class WritableSerial {
    public static void main(String[] args) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        IntWritable intWritable = new IntWritable(10101);

        byte[] serializeBytes = serialize(intWritable);

        System.out.println(Arrays.toString(serializeBytes));

        IntWritable deserializeIntWeitable = deserialize(IntWritable.class, serializeBytes);

        System.out.println(deserializeIntWeitable.get());
    }


    /**
     * 把writable类型的序列化
     * @param writable 传入的参，如IntWritable,LongWritable...
     * @return 序列化后的Byte字节流
     * @throws IOException 呵呵
     */
    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        writable.write(dataOutputStream);
        dataOutputStream.close();
        return outputStream.toByteArray();
    }

    /**
     * @param writable 传入需要返回的对象的class
     * @param bytes 序列化后的Byte字节流
     * @param <T> 及其优雅的范型
     * @return 返回序列化后的实例
     */
    public static <T extends Writable> T deserialize(Class<T> writable, byte[] bytes) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        Constructor<T> constructor = writable.getConstructor();
        T writableInstance = constructor.newInstance();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(inputStream);

        //将bytes字节流传入实例化后的Writable实例
        writableInstance.readFields(dataInputStream);

        return writableInstance;
    }
}

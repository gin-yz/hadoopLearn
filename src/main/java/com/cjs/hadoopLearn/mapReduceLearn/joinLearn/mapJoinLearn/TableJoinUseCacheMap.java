package com.cjs.hadoopLearn.mapReduceLearn.joinLearn.mapJoinLearn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class TableJoinUseCacheMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    HashMap<String,String> pdMap = new HashMap<>();
    Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //只存了一个ｃａｃｈｅ
        URI[] cacheFiles = context.getCacheFiles();

        String filePath = cacheFiles[0].getPath().toString();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] strings = line.split("\t");
            //ｋｅｙ:value
            pdMap.put(strings[0],strings[1]);
        }

        IOUtils.closeStream(bufferedReader);


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strings = value.toString().split("\t");

        String name = pdMap.get(strings[1]);

        String finalStr = strings[0]+"\t"+strings[2]+"\t"+name;

        v.set(finalStr);

        context.write(v,NullWritable.get());

    }
}

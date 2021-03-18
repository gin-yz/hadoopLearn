package com.cjs.hadoopLearn.mapReduceLearn.compressLearn;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;

public class Test {
    public static void main(String[] args) throws FileNotFoundException {
        String file = Test.class.getResource("./helloCJS.txt").getFile();

        System.out.println(file);
        FileInputStream fis = new FileInputStream(file);

    }
}

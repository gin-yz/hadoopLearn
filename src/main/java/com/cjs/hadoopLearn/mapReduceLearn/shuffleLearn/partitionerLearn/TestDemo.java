package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.partitionerLearn;


import java.net.URL;

public class TestDemo {
    public static void main(String[] args) {
        URL inputURL = TestDemo.class.getResource("phoneData .txt");
        System.out.println(inputURL);

    }
}

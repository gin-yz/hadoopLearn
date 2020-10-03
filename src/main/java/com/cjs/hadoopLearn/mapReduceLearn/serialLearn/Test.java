package com.cjs.hadoopLearn.mapReduceLearn.serialLearn;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class Test {
    public static void main(String[] args) throws IOException, URISyntaxException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(Test.class.getResource("./phoneData.txt").toURI())), StandardCharsets.UTF_8));

        Stream<String> stringStream = reader.lines();

        stringStream.map(new Function<String, List<String>>() {
            @Override
            public List<String> apply(String s) {

                String[] splitStr = s.split("\t");
                List<String> list = new ArrayList<>();
                for (String value : splitStr) {
//                    if (!value.equals("")) list.add(value.trim());
                    list.add(value.trim());
                }
                list.removeAll(Collections.singleton(" "));

                return list;
            }
        }).forEach(System.out::println);
    }
}

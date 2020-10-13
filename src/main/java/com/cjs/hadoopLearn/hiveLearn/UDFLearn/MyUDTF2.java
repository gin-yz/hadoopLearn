/*
* 自定义ｅｘｐｌｏｄｅ
*
* */

package com.cjs.hadoopLearn.hiveLearn.UDFLearn;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF2 extends GenericUDTF {
    private final transient Object[] forwardListObj = new Object[1];
    private transient ObjectInspector inputOI = null;
    @Override
    public StructObjectInspector initialize(StructObjectInspector
                                                        argOIs) throws UDFArgumentException {
        List<? extends StructField> fieldRefs = argOIs.getAllStructFieldRefs();
        this.inputOI = fieldRefs.get(0).getFieldObjectInspector();
        //1.定义输出数据的列名和类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        //2.添加输出数据的列名和类型
        fieldNames.add("lineToWord");

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        ListObjectInspector listOI = (ListObjectInspector)this.inputOI;
        List<?> list = listOI.getList(args[0]);

        for (Object r : list) {
            this.forwardListObj[0] = r;
            this.forward(this.forwardListObj);
        }

    }

    @Override
    public void close() throws HiveException {
    }
}


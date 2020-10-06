package com.cjs.hadoopLearn.mapReduceLearn.combineInReduceLearn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator{

	//必须要写构造方法，不然不会初始化ｋｅｙ的对象
	protected OrderGroupingComparator(){
		super(OrderBean.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 要求只要id相同，就认为是相同的key
		//这个只是为了让其分组，相同时返回０，不想同时返回随便的，上硅谷提供的下面的程序有误
		
		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;
		
		int result;
		if (aBean.getOrder_id() > bBean.getOrder_id()) {
			result = -1;
		}else if(aBean.getOrder_id() < bBean.getOrder_id()){
			result = 1;
		}else {
			result = 0;
		}

		return result;
	}
	
}

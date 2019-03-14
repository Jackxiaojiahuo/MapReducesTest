package com.geek.order.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: Jack Zhou
 * @Description: 自定义分组
 * @Date: Created in 20:28 2019/3/14
 */
public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        // 按照订单中的orderid来分发数据
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % i;
    }
}

package com.geek.join;

import com.geek.order.topn.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: Jack Zhou
 * @Description: 自定义分组
 * @Date: Created in 20:28 2019/3/14
 */
public class JoinPartitioner extends Partitioner<JoinBean, JoinBean> {

    @Override
    public int getPartition(JoinBean text, JoinBean joinBean, int i) {
        return (text.getUserId().hashCode() & Integer.MAX_VALUE) % i;
    }
}

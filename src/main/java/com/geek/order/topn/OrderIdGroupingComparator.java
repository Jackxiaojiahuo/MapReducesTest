package com.geek.order.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:30 2019/3/14
 */
public class OrderIdGroupingComparator extends WritableComparator {

    protected OrderIdGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean c = (OrderBean)a;
        OrderBean d = (OrderBean)b;

        return c.getOrderId().compareTo(d.getOrderId());
    }
}

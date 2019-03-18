package com.geek.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:30 2019/3/14
 */
public class JoinGroupingComparator extends WritableComparator {

    protected JoinGroupingComparator() {
        super(JoinBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinBean c = (JoinBean) a;
        JoinBean d = (JoinBean) b;

        return c.getUserId().compareTo(d.getUserId());
    }
}

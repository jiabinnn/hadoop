package org.example.mygrouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator {
    public OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 强制类型转换
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;

        // 指定分组规则
        return first.getOrderId().compareTo(second.getOrderId());
    }
}

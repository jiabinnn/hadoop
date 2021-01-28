package org.example.flow_count_partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartition extends Partitioner<Text, FlowBean> {
    // 用来指定分区规则
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // 手机号 K2 text
        // 获取手机号
        String phoneNum = text.toString();
        // 判断手机号以什么开头，返回对应的分区编号
        if(phoneNum.startsWith("135")) {
            return 0;
        }
        else if (phoneNum.startsWith("136")){
            return 1;
        }
        else if (phoneNum.startsWith("137")){
            return 2;
        }
        else{
            return 3;
        }

    }
}

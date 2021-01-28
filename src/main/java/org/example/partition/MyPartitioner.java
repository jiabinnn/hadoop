package org.example.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends Partitioner<Text, NullWritable> {

    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        //1 拆分行文本数据，获取中奖字段的值
        String[] split = text.toString().split("\t");
        //2 判断中奖字段的值和15的关系，然后返回对应的分区编号
        String numStr = split[5];
        if(Integer.parseInt(numStr) > 15){
            return 1;
        }
        else {
            return 0;
        }
    }
}

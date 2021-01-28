package org.example.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    K1: 行偏移量
    V1: 行文本数据

    K2: 一行文本数据
    V2: NullWirtable
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    //将K1 V1 转成K2 V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 定义计数器
        Counter counter = context.getCounter("MR_COUNTER", "partition_counter");
        counter.increment(1L);

        context.write(value, NullWritable.get());
    }
}

package org.example.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        // 遍历集合，将集合中的数字相加，得到V3
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }

        // 将K3 V3写入上下文中
        context.write(key, new LongWritable(count));
    }
}

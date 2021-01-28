package org.example.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    四个范型解释：
    KEYIN K2的类型
    VALUEIN V2的类型

    KEYOUT K3的类型
    VALUEOUT V3的类型
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    //将新的K2 V2转为K3 V3，将K3 V3写到上下文中
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

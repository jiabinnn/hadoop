package org.example.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    四个范型解释：
    KEYIN K1的类型
    VALUEIN V1的类型

    KEYOUT K2的类型
    VALUEOUT V2的类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    //将K1 V1 转成K2 V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将一行的文本数据进行拆分
        String[] split = value.toString().split(",");
        //遍历数组，组装K2 V2
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        for (String word : split) {
            text.set(word);
            longWritable.set(1);
            context.write(text, longWritable);

        }


        //将K2 V2写入上下文



    }
}

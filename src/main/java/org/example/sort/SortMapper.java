package org.example.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    // 将K1 V1 转为K2 V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将行文本数据（V1）拆分，将数据封装到SortBean对象，可到K2
        String[] split = value.toString().split("\t");
        SortBean sortBean = new SortBean();
        sortBean.setWord(split[0]);
        sortBean.setNum(Integer.parseInt(split[1]));

        // 将K2 V2写入上下文中
        context.write(sortBean, NullWritable.get());

    }
}

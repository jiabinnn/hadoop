package org.example.friend_step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 以冒号拆分文本数据，冒号左边是v2
        String[] split = value.toString().split(":");
        String userStr = split[0];

        // 冒号右边的字符串按逗号拆分，每个成员是k2
        String[] split1 = split[1].split(",");

        for (String s : split1) {
            context.write(new Text(s), new Text(userStr));
        }
    }
}

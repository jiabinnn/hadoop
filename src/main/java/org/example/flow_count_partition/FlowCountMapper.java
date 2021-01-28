package org.example.flow_count_partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 拆分行文本数据，得到数据
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];
        //2 创建FlowBean对象，并从行文本数据中拆分出流量到四个字段，并将四个流量字段到值赋给FlowBean对象
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));

        //3 将K2 V2写入上下文中
        context.write(new Text(phoneNum), flowBean);


    }
}

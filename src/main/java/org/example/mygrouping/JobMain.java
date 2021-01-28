package org.example.mygrouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.example.myoutputformat.MyOutputFormat;
import org.example.myoutputformat.MyOutputMapper;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        // 获取job对象
        Job job = Job.getInstance(super.getConf(), "mygroup_job");

        // 如果打包运行出错，则需要加该配置
        job.setJarByClass(JobMain.class);

        // 设置job任务
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/input/mygroup"));

        // 设置Mapper类和数据类型
        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        // 设置分区
        job.setPartitionerClass(OrderPartition.class);

        // 设置分组
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        //设置Reducer
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输出类和输出到路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/output/mygroup"));

        // 等待job任务结束
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}

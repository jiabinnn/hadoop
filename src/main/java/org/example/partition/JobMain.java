package org.example.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //1 创建job任务对象
        Job job = Job.getInstance(super.getConf(), "partition_mapreduce");
        // 如果打包运行出错，则需要加该配置
        job.setJarByClass(JobMain.class);
        //2 对job任务进行配置（8个步骤）
            //1 设置输入类和输入路径
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("hdfs://10.211.55.10:8020/input"));
        TextInputFormat.addInputPath(job, new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/input_partition"));

        //2 设置mapper类和数据类型
        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
            //3 指定分区类
        job.setPartitionerClass(MyPartitioner.class);
            //4 5 6 略

            //7 指定reducer类和数据类型
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
            // 设置reducetask对个数
        job.setNumReduceTasks(2);

            //8 设置输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
//        Path path = new Path("hdfs://10.211.55.10:8020/partition_out");
        Path path = new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/output_partition");

        TextOutputFormat.setOutputPath(job, path);

//        //获取文件系统 filesystem
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.211.55.10:8020"), new Configuration());
//        boolean exists = fileSystem.exists(path);//如果该目录存在，则删除该目录
//        if (exists){
//            fileSystem.delete(path, true);
//        }
        //3 等待任务结束
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 启动任务
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);

        System.exit(run);

    }
}

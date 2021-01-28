package org.example.flow_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.example.combiner.MyCombiner;
import org.example.combiner.WordCountMapper;
import org.example.combiner.WordCountReducer;

public class JobMain extends Configured implements Tool {

    // 该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        // 创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), "flow_count");
        // 如果打包运行出错，则需要加该配置
        job.setJarByClass(JobMain.class);


        // 第一步：读取输入文件解析成key，value对
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("hdfs://10.211.55.10:8020/input/flowcount"));
        TextInputFormat.addInputPath(job, new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/input/flowcount"));

        // 第二步：设置我们对mapper类
        job.setMapperClass(FlowCountMapper.class);
        // 设置我们map阶段完成之后对输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 第三四五六步省略

        // 第七步：设置我们第reduce类
        job.setReducerClass(FlowCountReducer.class);
        // 设置我们reduce阶段完成之后的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 第八步：设置输出类以及输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
//        Path path = new Path("hdfs://10.211.55.10:8020/out/flowcount_out");
//        TextOutputFormat.setOutputPath(job, path);
        TextOutputFormat.setOutputPath(job, new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/output/flowcount"));

//        //获取文件系统 filesystem
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.211.55.10:8020"), new Configuration());
//        boolean exists = fileSystem.exists(path);//如果该目录存在，则删除该目录
//        if (exists){
//            fileSystem.delete(path, true);
//        }
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 启动job任务
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}

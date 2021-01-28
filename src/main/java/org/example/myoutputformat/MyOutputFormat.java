package org.example.myoutputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 获取目标文件的输出流（2个）
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        FSDataOutputStream goodCommentsOutputStream = fileSystem.create(new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/output/myoutputformat/goodcomments/goodcomments.txt"));
        FSDataOutputStream badCommentsOutputStream = fileSystem.create(new Path("file:///Users/sunjiabin/IdeaProjects/mapreduce/output/myoutputformat/badcomments/badcomments.txt"));

        // 将输出流传给MyRecordWriter
        MyRecordWriter myRecordWriter = new MyRecordWriter(goodCommentsOutputStream, badCommentsOutputStream);



        return myRecordWriter;
    }
}

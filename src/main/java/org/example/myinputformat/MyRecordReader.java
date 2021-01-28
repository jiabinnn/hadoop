package org.example.myinputformat;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit = null;
    private Configuration configuration = null;
    private boolean processed = false;
    private BytesWritable bytesWritable = new BytesWritable();
    private FileSystem fileSystem = null;
    private FSDataInputStream inputStream = null;
    //初始化
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 获取文件到切片
        fileSplit = (FileSplit) inputSplit;

        // 获取configuration对象
        configuration = taskAttemptContext.getConfiguration();
    }

    // K1 NullWritable
    // V1 BytesWritable
    // 用于获取K1和V1
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed){// 获取源文件对字节输入流
            // 获取FileSystem
            fileSystem = FileSystem.get(configuration);
            // 通过FileSystem获取文件字节输入流
            inputStream = fileSystem.open(fileSplit.getPath());

            // 读取源文件数据到普通字节数组（byte[]）
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());

            // 将普通字节数组到数据封装到BytesWritable
            bytesWritable.set(bytes, 0, (int) fileSplit.getLength());

            processed = true;
            return true;
        }
        return false;
    }

    // 返回K1
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    // 返回V1
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    // 获取文件读取进度
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    // 资源释放
    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }
}

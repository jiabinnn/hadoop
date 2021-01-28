package org.example.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private int num;

    public void setWord(String word) {
        this.word = word;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getWord() {
        return word;
    }

    public int getNum() {
        return num;
    }

    @Override
    public String toString() {
        return word + '\t' + num;
    }

    // 实现比较器，指定排序规则
    @Override
    public int compareTo(SortBean sortBean) {
        // 先对第一列排序
        int result = this.word.compareTo(sortBean.word);

        // 如果第一列相同，则按照第二列进行排序
        if(result == 0){
            return this.num - sortBean.num;
        }
        return result;
    }

    // 实现序列号
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(num);

    }

    // 实现反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.word = in.readUTF();
        this.num = in.readInt();
    }
}

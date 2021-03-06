package com.geek.zhou;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:02 2018/11/19
 * <p>
 * KEYIN: 是map task读取到的数据的key类型,是一行的起始偏移量Long
 * VALUEIN: 是map task读取到的数据的value的类型,是一行的内容String
 * <p>
 * KEYOUT: 是用户自定义的map方法要返回的结果kv数据的key类型,在wordcount逻辑中,我们需要返回的是单词String
 * VALUEOUT: 是用户的自定义map方法要返回的kv数据的value类型,在wordcount逻辑中,我们需要返回的是整数Integer
 * <p>
 * 但是,在mapreduce中,map产生的数据需要传输给reduce,需要进行序列化和反序列化,而jdk中的原生序列化机制产生的数据量比较冗余,
 * 就会导致数据在mapreduce运行过程之中传输效率较低,所以,hadoop专门设计了自己的序列化机制,那么,在mapreduce中传输的数据类型就
 * 必须实现hadoop自己的序列化接口
 * <p>
 * hadoop为jdk中的常用基本类型Long String Integer Float等数据类型封装了自己的实现了hadoop序列化接口的类型:
 * LongWritable,Text,IntWritable,FloatWritable
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切单词
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }


}

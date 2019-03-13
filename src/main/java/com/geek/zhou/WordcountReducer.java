package com.geek.zhou;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:33 2018/11/19
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}

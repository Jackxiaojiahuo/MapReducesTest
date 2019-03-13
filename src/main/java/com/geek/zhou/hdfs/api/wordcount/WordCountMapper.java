package com.geek.zhou.hdfs.api.wordcount;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:54 2018/12/2
 */
public class WordCountMapper implements Mapper {
    @Override
    public void map(String line, Context context) {
        String[] words = line.split(" ");
        for (String word : words) {
            Object value = context.get(word);
            if (null == value) {
                context.write(word, 1);
            } else {
                int v = (int) value;
                context.write(word, ++v);
            }
        }
    }
}

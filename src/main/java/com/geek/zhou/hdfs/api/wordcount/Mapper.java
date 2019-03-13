package com.geek.zhou.hdfs.api.wordcount;

/**
 * @Author: ZhouZhiWei
 * @Description:
 * @Date: Created in 20:36 2018/12/2
 */
public interface Mapper {

    public void map(String line, Context context);
}

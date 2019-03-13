package com.geek.zhou.hdfs.datacollect;

import java.util.Timer;

/**
 * @Author: ZhouZhiWei
 * @Description:
 * @Date: Created in 22:24 2018/11/23
 */
public class DataCollectMain {
    public static void main(String[] args) {
        Timer timer = new Timer();

        timer.schedule(new CollectTask(), 0, 60*60*1000L);

        timer.schedule(new BackCleanTask(), 0, 60*60*1000);
    }
}

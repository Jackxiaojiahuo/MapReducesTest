package com.geek.zhou.hdfs.datacollect;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 15:28 2018/12/2
 */
public class BackCleanTask extends TimerTask {


    @Override
    public void run() {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        long now = new Date().getTime();
        // 探测备份目录
        File backupDir = new File("f:/logs/backup/");
        File[] dayBackDir = backupDir.listFiles();
        // 判断备份日期子目录是否已超24小时
        for (File dir : dayBackDir) {
            try {
                long time = sdf.parse(dir.getName()).getTime();
                if(now-time>24*60*60*1000L){
                    FileUtils.deleteDirectory(dir);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

package com.geek.zhou.hdfs.datacollect;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author: ZhouZhiWei
 * @Description: 单例设计, 方式二:懒汉式单例--考虑了线程安全
 * @Date: Created in 16:01 2018/12/2
 */
public class PropertyHodelLazy {
    private static Properties prop = null;

    public static Properties getProps() throws Exception {
        if (prop == null) {
            synchronized (PropertyHodelLazy.class) {
                if (prop == null) {
                    prop = new Properties();
                    prop.load(PropertyHodelHungery.class.getClassLoader().getResourceAsStream("collect.properties"));
                }
            }
        }
        return prop;
    }
}

package com.geek.zhou.hdfs.datacollect;

import java.util.Properties;

/**
 * @Author: ZhouZhiWei
 * @Description: 单例设计模式, 方式一:饿汉式单例
 * @Date: Created in 15:56 2018/12/2
 */
public class PropertyHodelHungery {

    private static Properties prop = new Properties();

    static{
        try {
            prop.load(PropertyHodelHungery.class.getClassLoader().getResourceAsStream("collect.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Properties getProps(){

        return prop;
    }

}

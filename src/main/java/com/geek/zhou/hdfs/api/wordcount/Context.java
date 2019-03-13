package com.geek.zhou.hdfs.api.wordcount;

import java.util.HashMap;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:38 2018/12/2
 */
public class Context {

    private HashMap<Object, Object> contextMap = new HashMap<Object, Object>();

    public void write(Object key, Object value) {
        contextMap.put(key, value);
    }

    public Object get(Object key){
        return contextMap.get(key);
    }

    public HashMap<Object, Object> getContextMap(){
        return contextMap;
    }
}

package com.geek.zhou.hdfs.api.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:28 2018/12/2
 */
public class HdfsWordCount {
    public static void main(String[] args) throws Exception {

        /**
         * 初始化数据
         */
        Properties props = new Properties();
        props.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties"));

        Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS"));
        Mapper mapper = (Mapper) mapper_class.newInstance();

        Context context = new Context();
        Path inPath = new Path(props.getProperty("INPUT_PATH"));
        Path outPath = new Path(props.getProperty("OUTPUT_PATH"));


        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), new Configuration(), "root");
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(inPath, false);

        while (iter.hasNext()) {
            LocatedFileStatus file = iter.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
            String line = null;
            // 去HDFS中读文件:一次读一行
            // 逐行读取
            while ((line = br.readLine()) != null) {
                // 调用一个方法对每一行进行业务处理
                mapper.map(line, context);
            }
            br.close();
            in.close();
        }

        /**
         * 输出结果
         */
        HashMap<Object, Object> contextMap = context.getContextMap();
        if (fs.exists(outPath)) {
            throw new RuntimeException("指定的输出目录已存在,请更换...");
        }
        // 调用一个方法将缓存中的结果数据输出到HDFS结果文件
        FSDataOutputStream out = fs.create(new Path(outPath, "res.dat"));
        Set<Map.Entry<Object, Object>> entrySet = contextMap.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            out.write(((entry.getKey().toString() + "\t" + entry.getValue() + "\n")).getBytes());
        }
        out.close();
        fs.close();
        System.out.println("恭喜数据统计完成");



    }
}

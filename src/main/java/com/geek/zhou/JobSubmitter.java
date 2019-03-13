package com.geek.zhou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @Author: Jack Zhou
 * @Description: 用于提交mapreduce job的客户端程序
 * 功能:
 * 1.封装本次job 运行时所需要的必要参数
 * 2.跟yarn进行交互,将mapreduce程序成功的启动,运行
 * hadoop jar xx.jar com.geek.zhou.JobSubmitter 启动客户端main方法
 * @Date: Created in 21:41 2018/11/20
 */
public class JobSubmitter {
    public static void main(String[] args) throws Exception {

        // 在代码中设置JVM系统参数,用于给job对象来获取访问HDFS的用户信息
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        // 1.设置job运行时要访问的默认文件系统
        conf.set("fs.default.name", "hdfs://master:9000");
        // 2.设置job提交到哪去运行
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");
        // 3.如果要从windows系统上运行这个job提交客户端程序,则需要加这个跨平台提交的参数
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf);
        // 1.封装参数:jar包所在的位置
//        job.setJar("f:/wc.jar");
        job.setJarByClass(JobSubmitter.class);
        // 2.封装参数:本次job所要调用的Mapper实现类、Reducer实现类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 3.封装参数:本次job的Mapper实现类产生的结果数据的key,value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path output = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        // 4.封装参数:本次job要处理的输入数据集所在路径、最终结果的输出路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        // 5.封装参数:想要启动的reduceTask的数量
        job.setNumReduceTasks(2);

        // 6.提交job给yarn
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}

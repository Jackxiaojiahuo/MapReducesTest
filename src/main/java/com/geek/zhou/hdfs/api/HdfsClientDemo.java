package com.geek.zhou.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 20:53 2018/11/22
 */
public class HdfsClientDemo {
    public static void main(String[] args) throws Exception {
        /**
         * Configuration参数对象的机制:
         * 构造时:会加载jar包中的默认配置
         * 再加载,用户配置xx-site.xml,覆盖默认参数
         * 构造完成后,还可以conf.set(k,v),会再次覆盖用户配置文件中的参数值
         */
        // new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml中查找配置信息
        Configuration conf = new Configuration();
        // 指定本客户端上传文件到hdfs时需要保存的副本数为:2
        conf.set("dfs.replication", "2");
        // 指定本客户端上传那文件到hdfs时切块的规格大小:64M
        conf.set("dfs.blocksize", "64m");
        // 构造一个访问指定HDFS系统的客户端对象:参数1:--HDFS系统的URI,参数2:--客户端要特别指定的参数,参数3:客户端的身份(用户名)
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000/"), conf, "root");

        // 上传一个文件到hdfs
        fs.copyFromLocalFile(new Path("F:/jdk_demo.tar.gz"), new Path("/jdk_demo.tar.gz"));

        fs.close();
    }

    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");
        fs = FileSystem.get(new URI("hdfs://master:9000/"), conf, "root");
    }

    /**
     * 从HDFS上下载文件到本地
     *
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {
        fs.copyToLocalFile(new Path("/README.txt"), new Path("F:/readme.txt"));
        fs.close();
    }

    /**
     * 在HDFS上内部移动文件
     */
    @Test
    public void testRname() throws IOException {
        fs.rename(new Path("/aa.txt"), new Path("/aaa/bb.txt"));
        fs.close();
    }

    /**
     * 在HDFS上创建文件夹
     *
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException {
        fs.mkdirs(new Path("/xx/yy/zz"));
        fs.close();
    }

    /**
     * 在HDFS上删除文件夹或文件
     *
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {
        fs.delete(new Path("/aaa"), true);
        fs.close();
    }

    /**
     * 查询HDFS指定目录下的文件信息
     *
     * @throws IOException
     */
    @Test
    public void testLs() throws IOException {
        // 只查询文件的信息,不返回文件夹的信息
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
        while (iter.hasNext()) {
            LocatedFileStatus next = iter.next();
            System.out.println("文件全路径:" + next.getPath());
            System.out.println("块大小:" + next.getBlockSize());
            System.out.println("文件长度:" + next.getLen());
            System.out.println("副本数量:" + next.getReplication());
            System.out.println("块数量:" + Arrays.toString(next.getBlockLocations()));
            System.out.println("-------------------------------------------------------");
        }
        fs.close();
    }

    /**
     * 查询HDFS指定目录下的文件夹和文件信息
     *
     * @throws IOException
     */
    @Test
    public void testLs2() throws IOException {

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("文件全路径:" + fileStatus.getPath());
            System.out.println(fileStatus.isDirectory() ? "这是文件夹" : "这是文件");
            System.out.println("块大小:" + fileStatus.getBlockSize());
            System.out.println("文件长度:" + fileStatus.getLen());
            System.out.println("副本数量:" + fileStatus.getReplication());
            System.out.println("-------------------------------------------------------");
        }
        fs.close();
    }

    /**
     * 读取HDFS中的文件内容
     */
    @Test
    public void testReadDataBuffered() throws IOException {

        FSDataInputStream in = fs.open(new Path("/test.txt"));
        // 字符流
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();
        in.close();
        fs.close();


    }

    /**
     * 读取HDFS中的文件内容
     */
    @Test
    public void testReadDataByte() throws IOException {

        FSDataInputStream in = fs.open(new Path("/test.txt"));

        // 字节流
        byte[] buf = new byte[1024];
        in.read(buf);
        System.out.println(new String(buf));

        in.close();
        fs.close();

    }

    /**
     * 读取HDFS中的文件内容
     */
    @Test
    public void testRandomReadData() throws IOException {

        FSDataInputStream in = fs.open(new Path("/xx.dat"));
        // 将读取的起始位置指定
        in.seek(12);

        // 读16个字节
        byte[] buf = new byte[16];
        in.read(buf);
        System.out.println(new String(buf));

        in.close();
        fs.close();

    }

    /**
     * 往HDFS中的文件写内容
     */
    @Test
    public void testWriteData() throws IOException {

        //F:\logs\hospital.png
        FSDataOutputStream out = fs.create(new Path("/yy.jpg"), true);

        FileInputStream in = new FileInputStream("F:/logs/hospital.jpg");

        byte[] buf = new byte[1024];
        int read = 0;
        while ((read = in.read()) != -1) {
            out.write(buf, 0, read);
        }
        in.close();
        out.close();
        fs.close();
    }

    /**
     * 往HDFS中的文件写内容
     */
    @Test
    public void testMe() {
        String[] str = "08:05:00".split(":");
        int a = Integer.valueOf(str[0]);
        int b = Integer.valueOf(str[1]);
        int c = Integer.valueOf(str[2]);
        System.out.println(a + "  " + b+"  "+c);
    }


}

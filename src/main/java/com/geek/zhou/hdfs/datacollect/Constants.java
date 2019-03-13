package com.geek.zhou.hdfs.datacollect;

/**
 * @Author: ZhouZhiWei
 * @Description:
 * @Date: Created in 16:11 2018/12/2
 */
public class Constants {

    /**
     * 日志源路径的key
     */
    public static final String LOG_SOURCE_DIR = "LOG_SOURCE_DIR";
    /**
     * 待上传临时目录的key
     */
    public static final String LOG_TOUPLOGD_DIR = "LOG_TOUPLOGD_DIR";
    /**
     * 备份目录的key
     */
    public static final String LOG_BACKUP_BASE_DIR = "LOG_BACKUP_BASE_DIR";
    /**
     * 采集文件的合法前缀的key
     */
    public static final String LOG_LEGAL_PREFIX = "LOG_LEGAL_PREFIX";
    /**
     * 删除备份文件的超时时间的key
     */
    public static final String LOG_BACKUP_TIMEOUT = "LOG_BACKUP_TIMEOUT";
    /**
     * HDFS存储路径的key
     */
    public static final String HDFS_DEST_BASE_DIR = "HDFS_DEST_BASE_DIR";
    /**
     * HDFS的URI的key
     */
    public static final String HDFS_URI = "HDFS_URI";
    /**
     * HDFS中的文件的前缀的key
     */
    public static final String HDFS_FILE_PREFIX = "HDFS_FILE_PREFIX";
    /**
     * HDFS中的文件的后缀的key
     */
    public static final String HDFS_FILE_SUFFIX = "HDFS_FILE_SUFFIX";
}

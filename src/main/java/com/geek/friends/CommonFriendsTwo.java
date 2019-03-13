package com.geek.friends;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @Author: Jack Zhou
 * @Description: 共同好友统计案例实现
 * @Date: Created in 20:32 2019/3/13
 */
public class CommonFriendsTwo {
    public static class CommonFriendsTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        // B-C	A    B-D	A
        // 输出: B-C	A B-D	A ...

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] userAndFriends = value.toString().split("\t");
            context.write(new Text(userAndFriends[0]), new Text(userAndFriends[1]));
        }
    }

    public static class CommonFriendsTwoReducer extends Reducer<Text, Text, Text, Text> {

        // 一组数据: B-C	A
        // 一组数据: B-C	D

        @Override
        protected void reduce(Text friend, Iterable<Text> users, Context context) throws IOException, InterruptedException {
            ArrayList<String> userList = new ArrayList<String>();
            for (Text user : users) {
                userList.add(user.toString());
            }
            Collections.sort(userList);
            context.write(friend, new Text(StringUtils.join(userList, ",")));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CommonFriendsTwo.class);

        job.setMapperClass(CommonFriendsTwoMapper.class);
        job.setReducerClass(CommonFriendsTwoReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\datas\\friends\\output"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\datas\\friends\\output1"));

        job.waitForCompletion(true);
    }
}

package com.geek.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: Jack Zhou
 * @Description:
 * @Date: Created in 19:14 2019/3/18
 */
public class ReduceSideJoin {

    public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, JoinBean, JoinBean> {
        String fileName = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            fileName = inputSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            JoinBean bean = new JoinBean();
            if (fileName.startsWith("order")) {
                bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
            } else {
                bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
            }
            context.write(bean, bean);

        }
    }


    public static class ReduceSideJoinReducer extends Reducer<JoinBean, JoinBean, JoinBean, NullWritable> {

        @Override
        protected void reduce(JoinBean key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
            JoinBean newBean = new JoinBean();


            // 区分两类数据
            int i = 0;
            for (JoinBean value : values) {
                if (++i > 1) {
                    value.setUserName(newBean.getUserName());
                    value.setUserAge(newBean.getUserAge());
                    value.setUserFriend(newBean.getUserFriend());
                    context.write(value, NullWritable.get());
                } else {
                    newBean.setUserName(value.getUserName());
                    newBean.setUserAge(value.getUserAge());
                    newBean.setUserFriend(value.getUserFriend());
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ReduceSideJoin.class);

        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setReducerClass(ReduceSideJoinReducer.class);

        job.setPartitionerClass(JoinPartitioner.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(JoinBean.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\datas\\join\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\datas\\join\\output"));

        job.waitForCompletion(true);
    }
}

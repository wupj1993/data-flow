package com.wupj.bd.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 〈用户统计〉
 *
 * @author wupeiji
 * @date 2019/5/17 14:40
 * @since 1.0.0
 */
public class UserMetric {

    /**
     * 用户mapper
     */
    static class UserMapper extends Mapper<Object, Text, Text, UserMsg> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String[] split = value.toString().split(",");
            if (split == null || split.length != 3) {
                System.out.println("垃圾数据，不要也罢" + value.toString());
            }
            String name = split[0];
            String address = split[1];
            String ageStr = split[2];
            int age = -1;
            try {
                age = Integer.parseInt(ageStr);
            } catch (NumberFormatException e) {
                System.out.println("年龄错误" + ageStr);
            }
            UserMsg userMsg = new UserMsg(name, address, age);
            context.write(new Text(name), userMsg);
        }
    }

    static class UserReduce extends Reducer<Text, UserMsg, Text, IntWritable> {
        IntWritable child = new IntWritable(0);
        IntWritable young = new IntWritable(0);
        IntWritable old = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<UserMsg> values, Context context) throws IOException, InterruptedException {
            int childInt = 0;
            int youngInt = 0;
            int oldInt = 0;
            for (UserMsg userMsg : values) {
                System.out.println(userMsg.toString());
                final Integer age = userMsg.getAge();
                if (age < 18 && age > 0) {
                    childInt++;
                } else if (age >= 18 && age < 28) {
                    youngInt += 1;
                } else {
                    oldInt += 1;
                }
            }
            child.set(childInt);
            young.set(youngInt);
            old.set(oldInt);
            context.write(new Text("child"), child);
            context.write(new Text("young"), young);
            context.write(new Text("old"), old);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "user");
        job.setJarByClass(UserMetric.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReduce.class);

        // 这几个类型要指定不否可能报：类型不匹配的问题
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserMsg.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        final File file = new File(args[1]);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

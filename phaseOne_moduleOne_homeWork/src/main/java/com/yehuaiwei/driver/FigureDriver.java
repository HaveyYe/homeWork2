package com.yehuaiwei.driver;

import com.yehuaiwei.mapper.FigureMapper;
import com.yehuaiwei.reduce.FigureReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: yehw
 * @date: 2020/7/7 01:00
 * @description: 驱动类
 */
public class FigureDriver {
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //定义配置类
        Configuration configuration=new Configuration();
        Job job= Job.getInstance(configuration);

        //设置驱动包所在路径®R
        job.setJarByClass(FigureDriver.class);

        //设置mapper
        job.setMapperClass(FigureMapper.class);
        job.setReducerClass(FigureReduce.class);

        //设置Mapper输出kv类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置Reduce输出kv类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        //定义输入输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //启动
        boolean flag=job.waitForCompletion(true);
        System.exit(flag?0:1);

    }
}

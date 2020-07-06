package com.yehuaiwei.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: yehw
 * @date: 2020/7/7 00:55
 * @description: 数字排序统计Mapper类
 */
public class FigureMapper extends Mapper<LongWritable, Text, LongWritable,LongWritable> {
     LongWritable longWritable=new LongWritable();


    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //第一个数据
        String s = value.toString();
        //负值是value
        LongWritable val=new LongWritable();
        val.set(Long.valueOf(s));
        context.write(longWritable,val);
    }

}

package com.yehuaiwei.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: yehw
 * @date: 2020/7/7 00:57
 * @description: 数字排序统计Reduce
 */
public class FigureReduce extends Reducer<LongWritable, LongWritable, IntWritable, LongWritable> {
    List<Long> list = new ArrayList<Long>();
    IntWritable intWritable = new IntWritable();
    LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable value : values) {
            long l = value.get();
            System.out.println("reduce:" + l);
            list.add(Long.valueOf(l));
        }
        //排序
        list.sort((o1,o2)-> {
            return o1.compareTo(o2);
        });
        for (int i = 0; i < list.size(); i++) {
            intWritable.set(i + 1);
            longWritable.set(list.get(i));
            context.write(intWritable, longWritable);
        }
    }

}

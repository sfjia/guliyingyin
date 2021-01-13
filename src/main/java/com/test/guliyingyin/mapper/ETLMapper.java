package com.test.guliyingyin.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/11/89:37
 */
public class ETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //读物数据
        String line = value.toString();

        //清洗
        String etlStr = ETLUtil.etlStr(line);
        //写入
        if(StringUtils.isBlank(etlStr)){
            return;
        }
        text.set(etlStr);
        context.write(text,NullWritable.get());

    }
}

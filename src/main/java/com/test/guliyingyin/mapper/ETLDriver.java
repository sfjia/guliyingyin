package com.test.guliyingyin.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author 贾
 * @Date 2020/11/810:19
 */
public class ETLDriver implements Tool {

    private Configuration configuration;

    public int run(String[] arg) throws Exception {
        //1.获取job
        Job job = Job.getInstance(getConf());
        //2.封装driver类
        job.setJarByClass(ETLDriver.class);
        //3 封装mapper类
        job.setMapperClass(ETLMapper.class);
        //4.封装 输入KV输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.输入输出路径
        FileInputFormat.setInputPaths(job,new Path(arg[0]));
        FileOutputFormat.setOutputPath(job,new Path(arg[1]));

        //7 提价任务
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public void setConf(Configuration configuration) {
        this.configuration=configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args)throws Exception {
        int i = ToolRunner.run(new ETLDriver(), args);
        System.out.println("i = " + i);
    }
}

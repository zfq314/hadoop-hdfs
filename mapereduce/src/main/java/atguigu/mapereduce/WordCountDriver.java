package atguigu.mapereduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置信息及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //设置jar加载路径
        job.setJarByClass(WordCountDriver.class);
        //设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setInputFormatClass(CombineFileInputFormat.class);
        //设置map输出,Reucer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的Kv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交
        boolean result = job.waitForCompletion(true);
    }
}

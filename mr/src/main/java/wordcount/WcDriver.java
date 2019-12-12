package wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * WcMapper
 *
 * @author zhaofuqiang
 * @date 2019/11/17 22:05
 */
public class WcDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 生成一个job实例
    Job job = Job.getInstance(new Configuration());
    // 设置类路径
    job.setJarByClass(WcDriver.class);
    // 设置这个job的mapper和Reduce
    job.setMapperClass(WcMapper.class);
    job.setReducerClass(WcReducer.class);
    // 设置这个job的mapper和Reduce输出类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // 设置的程序的输入和输出
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // 提交任务
    // 里面设置成true就是打印详细信息
    // b就是人任务执行的结果
    boolean b = job.waitForCompletion(true);
    System.exit(b ? 0 : 1);
  }
}

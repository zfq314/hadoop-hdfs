package atguigu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // jar包
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(WordCountDriver.class);

    // 设置map和reducer
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReduce.class);

    // map的输出和输入
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // 最终的输出和输入
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // 如果不设置InputFormat，它默认用的是TextInputFormat.class
    job.setInputFormatClass(CombineTextInputFormat.class);

    // 虚拟存储切片最大值设置4m
    CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

    // 设置输出和输入
    FileInputFormat.setInputPaths(job, new Path("F:/idea/input"));
    FileOutputFormat.setOutputPath(job, new Path("F:/idea/output"));
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}

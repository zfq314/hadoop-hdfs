package alibaba.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 驱动类加载
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(WordCountDriver.class);

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.setInputPaths(job, new Path("d:/idea/input"));
    FileOutputFormat.setOutputPath(job, new Path("d:/idea/reoutput"));
    boolean b = job.waitForCompletion(true);
    System.exit(b ? 0 : 1);
  }
}

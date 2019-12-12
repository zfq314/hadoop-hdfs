package atguigu.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NLineDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration);

    // 7设置每个切片InputSplit中划分三条记录
    NLineInputFormat.setNumLinesPerSplit(job, 3);
    // 8使用NLineInputFormat处理记录数
    job.setInputFormatClass(NLineInputFormat.class);
    job.setJarByClass(NLineDriver.class);

    // map和reduce
    job.setMapperClass(NLineMapper.class);
    job.setReducerClass(NLineReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    // 删除文件

    FileInputFormat.setInputPaths(job, new Path("F:/idea/nline"));
    FileOutputFormat.setOutputPath(job, new Path("F:/idea/nlineout"));
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}

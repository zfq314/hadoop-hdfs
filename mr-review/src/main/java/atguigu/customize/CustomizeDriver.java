package atguigu.customize;


import atguigu.utils.DelTempDirUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class CustomizeDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration);
    job.setJarByClass(CustomizeDriver.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);

    job.setInputFormatClass(CustomizeFileInputFormat.class);
    job.setOutputValueClass(SequenceFileOutputFormat.class);
    DelTempDirUtils.clean("F:/idea/customizeout");
    FileInputFormat.setInputPaths(job, new Path("F:/idea/customize"));
    FileOutputFormat.setOutputPath(job, new Path("F:/idea/customizeout"));
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}

package atguigu.combiner;

import atguigu.utils.DelTempDirUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * CombinerDriver
 *
 * @author zhaofuqiang
 * @date 2019/10/7 18:38
 */
public class CombinerDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(CombinerDriver.class);

    job.setMapperClass(CombinerMapper.class);
    job.setReducerClass(MyCombiner.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setCombinerClass(MyCombiner.class);
    DelTempDirUtils.clean("f:/idea/CombinerOut");
    FileInputFormat.setInputPaths(job, new Path("f:/idea/Combiner"));
    FileOutputFormat.setOutputPath(job, new Path("f:/idea/CombinerOut"));
    boolean completion = job.waitForCompletion(true);
    System.exit(completion ? 0 : 1);
  }
}

package atguigu.groupingcomparator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * OrderDriver
 *
 * @author zhaofuqiang
 * @date 2019/10/7 21:20
 */
public class OrderDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(OrderDriver.class);
    job.setMapperClass(OrderMapper.class);
    job.setReducerClass(OrderReducer.class);

    job.setMapOutputKeyClass(OrderBean.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(OrderBean.class);
    job.setOutputValueClass(NullWritable.class);

    job.setGroupingComparatorClass(OrderComparator.class);
    FileInputFormat.setInputPaths(job, new Path("f:/idea/GroupingComparator"));
    FileOutputFormat.setOutputPath(job, new Path("f:/idea/GroupingComparatorOut"));
    boolean completion = job.waitForCompletion(true);
    System.exit(completion ? 0 : 1);
  }
}

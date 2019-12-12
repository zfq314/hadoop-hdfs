package alibaba.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowBeanDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(FlowBeanDriver.class);

    job.setMapperClass(FlowBeanMapper.class);
    job.setReducerClass(FlowBeanReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlowBean.class);
    // job.setPartitionerClass(FlowPartitionerPartitioner.class);
    // job.setNumReduceTasks(5);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);

    FileInputFormat.setInputPaths(job, new Path("d:/flowbean"));
    FileOutputFormat.setOutputPath(job, new Path("d:/flowbeanOutput"));
    boolean b = job.waitForCompletion(true);
    System.exit(b ? 0 : 1);
  }
}

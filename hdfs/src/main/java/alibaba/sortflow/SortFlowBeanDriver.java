package alibaba.sortflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortFlowBeanDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(SortFlowBeanDriver.class);

    job.setMapperClass(SortFlowBeanMapper.class);
    job.setReducerClass(SortFlowBeanReducer.class);

    job.setMapOutputKeyClass(FlowBean.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);

    FileInputFormat.setInputPaths(job, new Path("d:/flowbeanOutput"));
    FileOutputFormat.setOutputPath(job, new Path("d:/ToflowbeanOutput"));
    boolean b = job.waitForCompletion(true);
    System.exit(b ? 0 : 1);
  }
}

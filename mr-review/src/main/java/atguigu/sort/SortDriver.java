package atguigu.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * SortFDriver
 *
 * @author zhaofuqiang
 * @date 2019/10/7 14:43
 */
public class SortDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    //
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(SortDriver.class);

    job.setMapperClass(SortFlowMapper.class);
    job.setReducerClass(SortFlowReducer.class);
    job.setMapOutputKeyClass(SortFlowBean.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SortFlowBean.class);

    job.setPartitionerClass(SortPartition.class);
    job.setNumReduceTasks(5);
    // 输入和输出路径
    FileInputFormat.setInputPaths(job, new Path("f:/idea/SerializationOutput"));
    FileOutputFormat.setOutputPath(job, new Path("f:/idea/SerializationOutput_out"));
    boolean completion = job.waitForCompletion(true);
    System.exit(completion ? 0 : 1);
  }
}

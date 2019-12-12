package atguigu.partition;


import atguigu.flow.FlowBean;
import atguigu.flow.FlowMapper;
import atguigu.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * PartitionDriver
 *
 * @author zhaofuqiang
 * @date 2019/10/7 13:26
 */
public class PartitionDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    //
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(PartitionDriver.class);

    // 所操作的业务类
    job.setMapperClass(FlowMapper.class);
    job.setReducerClass(FlowReducer.class);

    // map的输入和输出
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlowBean.class);
    // 最终的输入和输出
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);
    // 指定自定义分区
    job.setPartitionerClass(TelephoneNumPartition.class);
    // 设置reduceTask
    job.setNumReduceTasks(5);
    FileInputFormat.setInputPaths(job, new Path("F:/idea/Partition"));
    FileOutputFormat.setOutputPath(job, new Path("f:/idea/PartitionOut"));
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}

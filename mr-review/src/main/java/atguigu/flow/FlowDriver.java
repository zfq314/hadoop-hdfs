package atguigu.flow;

import atguigu.utils.DelTempDirUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(FlowDriver.class);

    // 设置mapper和reducer
    job.setMapperClass(FlowMapper.class);
    job.setReducerClass(FlowReducer.class);

    // map的输入和输出
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlowBean.class);

    // 最终的输入和输出
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);
    DelTempDirUtils.clean("F:/idea/SerializationOutput");

    // 文件的输入和输出路径，
    FileInputFormat.setInputPaths(job, new Path("F:/idea/Serialization"));
    FileOutputFormat.setOutputPath(job, new Path("F:/idea/SerializationOutput"));
    // 最终任务的提交
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}

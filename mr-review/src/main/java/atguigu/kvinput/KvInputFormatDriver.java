package atguigu.kvinput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KvInputFormatDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = new Configuration();
    // 设置切割符
    // 设置行的分隔符，这里是空格符，第一个空格符前面的是Key，第一个空格符后面的内容都是value
    configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
    Job job = Job.getInstance(configuration);
    job.setJarByClass(KvInputFormatDriver.class);
    // map和reduced的加载类
    job.setMapperClass(KvInputFormatMapper.class);
    job.setReducerClass(KvInputFormatReducer.class);
    // map的输入和输出的类
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    // reduce的输出和输入
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    // 删除输出的路径
    // 输入和输出的数据
    FileInputFormat.setInputPaths(job, new Path("F:/idea/kv"));
    // 设置输入格式
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path("F:/idea/kvout"));
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}

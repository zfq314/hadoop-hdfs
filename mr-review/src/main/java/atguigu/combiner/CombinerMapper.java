package atguigu.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * CombinerMapper
 *
 * @author zhaofuqiang
 * @date 2019/10/7 18:38
 */
public class CombinerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  // 输出的key
  private Text k = new Text();
  // 输出的value
  private IntWritable v = new IntWritable(1);
  // 重写maper方法

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split(" ");
    for (String s : split) {
      k.set(s);
      context.write(k, v);
    }
  }
}

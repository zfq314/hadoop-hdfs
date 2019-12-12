package atguigu.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MyCombiner
 *
 * @author zhaofuqiang
 * @date 2019/10/7 18:38
 */
public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable v = new IntWritable();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    // 遍历value
    for (IntWritable value : values) {
      sum += value.get();
    }
    v.set(sum);
    context.write(key, v);
  }
}

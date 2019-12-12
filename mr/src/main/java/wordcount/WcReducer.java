package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * WcMapper
 *
 * @author zhaofuqiang
 * @date 2019/11/17 22:05
 */
public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable v = new IntWritable();
  /**
   * reduce的核心逻辑
   *
   * @param key 一组的单词
   * @param values 这个单词的好多个1
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }
    v.set(sum);
    context.write(key, v);
  }
}

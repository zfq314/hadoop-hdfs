package atguigu.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  // 设置输入的值
  private LongWritable v = new LongWritable();

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0L;
    for (LongWritable value : values) {
      sum += value.get();
    }
    v.set(sum);
    // 设置输出
    context.write(key, v);
  }
}

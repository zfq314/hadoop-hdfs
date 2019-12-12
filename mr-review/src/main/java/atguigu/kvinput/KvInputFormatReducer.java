package atguigu.kvinput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KvInputFormatReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  private LongWritable v = new LongWritable();

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0L;
    for (LongWritable value : values) {
      sum += value.get();
    }
    v.set(sum);
    // 输出
    context.write(key, v);
  }
}

package atguigu.kvinput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KvInputFormatMapper extends Mapper<Text, Text, Text, LongWritable> {
  private LongWritable v = new LongWritable(1);

  @Override
  protected void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    // 写入key，v
    context.write(key, v);
  }
}

package atguigu.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  // 设置key
  private Text k = new Text();
  // 设置输出vaLue
  private LongWritable v = new LongWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // 遍历一行
    String line = value.toString();
    // 切分
    String[] split = line.split(" ");
    // 循环写出
    for (int i = 0; i < split.length; i++) {
      k.set(split[i]);
      context.write(k, v);
    }
  }
}

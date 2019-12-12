package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WcMapper
 *
 * @author zhaofuqiang
 * @date 2019/11/17 22:05
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private Text k = new Text();
  private IntWritable v = new IntWritable(1);
  /**
   * map方法是MapTask的核心逻辑 ，对于每组输入的kv都会执行一次
   *
   * @param key 行的偏移量
   * @param value 行的内如
   * @param context 任务本身
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 将一行内容切分成一组单词

    String[] words = value.toString().split(" ");
    // 转换单词的输出格式‘
    for (String word : words) {
        k.set(word);
        context.write(k,v);
    }
  }
}

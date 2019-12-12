package atguigu.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * SortFlowMapper
 *
 * @author zhaofuqiang
 * @date 2019/10/7 14:48
 */
public class SortFlowMapper extends Mapper<LongWritable, Text, SortFlowBean, Text> {
  private SortFlowBean k = new SortFlowBean();
  private Text v = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");
    // 封装对象
    String phoneNum = split[0];
    long upFlow = Long.parseLong(split[1]);
    long downFlow = Long.parseLong(split[2]);
    k.set(upFlow, downFlow);
    v.set(phoneNum);
    context.write(k, v);
  }
}

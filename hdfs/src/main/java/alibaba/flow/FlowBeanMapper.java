package alibaba.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
  private FlowBean v = new FlowBean();
  private Text k = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 读取一行数据
    String[] split = value.toString().split("\t");

    k.set(split[1]);
    long upFlow = Long.parseLong(split[split.length - 3]);
    long downFlow = Long.parseLong(split[split.length - 2]);
    v.set(upFlow, downFlow);
    context.write(k, v);
  }
}

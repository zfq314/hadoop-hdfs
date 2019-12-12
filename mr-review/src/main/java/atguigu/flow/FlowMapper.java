package atguigu.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
  // key
  private Text k = new Text();
  // value
  private FlowBean flowBean = new FlowBean();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 切割对象
    String[] split = value.toString().split("\t");
    // 封装对象
    String phone = split[1];
    // 取出上行流量
    long upFlow = Long.parseLong(split[split.length - 3]);
    long downFlow = Long.parseLong(split[split.length - 2]);
    flowBean.set(upFlow, downFlow);
    k.set(phone);
    context.write(k, flowBean);
  }
}

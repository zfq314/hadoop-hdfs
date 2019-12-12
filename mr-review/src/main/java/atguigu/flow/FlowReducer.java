package atguigu.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
  private FlowBean flowBean = new FlowBean();

  @Override
  protected void reduce(Text key, Iterable<FlowBean> values, Context context)
      throws IOException, InterruptedException {
    long sum_upFlow = 0;
    long sum_downFlow = 0;
    for (FlowBean value : values) {
      sum_upFlow = value.getUpFlow();
      sum_downFlow = value.getDownFlow();
    }
    // 封装成对象
    flowBean.set(sum_upFlow, sum_downFlow);
    // 写出数据
    context.write(key, flowBean);
  }
}

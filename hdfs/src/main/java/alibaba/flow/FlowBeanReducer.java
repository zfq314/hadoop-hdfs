package alibaba.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowBeanReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
  private FlowBean v = new FlowBean();
  /**
   * @param key 电话号码
   * @param values 封装的javabean对象
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void reduce(Text key, Iterable<FlowBean> values, Context context)
      throws IOException, InterruptedException {
    long summUpFlow = 0;
    long sumDownFlow = 0;
    for (FlowBean value : values) {
      summUpFlow = value.getUpFlow();
      sumDownFlow = value.getDownFlow();
    }
    v.set(summUpFlow, sumDownFlow);

    context.write(key, v);
  }
}

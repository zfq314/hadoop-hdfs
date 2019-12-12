package atguigu.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * SortFlowReducer
 *
 * @author zhaofuqiang
 * @date 2019/10/7 14:48
 */
public class SortFlowReducer extends Reducer<SortFlowBean, Text, Text, SortFlowBean> {
  @Override
  protected void reduce(SortFlowBean key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    // 循环输出，避免流量相同的情况
    for (Text value : values) {
      context.write(value, key);
    }
  }
}

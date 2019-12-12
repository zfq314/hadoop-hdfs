package atguigu.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * OrderReducer
 *
 * @author zhaofuqiang
 * @date 2019/10/7 21:17
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
  @Override
  protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {

    context.write(key, NullWritable.get());
  }
}

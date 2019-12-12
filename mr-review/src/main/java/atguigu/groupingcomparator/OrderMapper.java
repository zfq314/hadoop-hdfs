package atguigu.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * OrderMapper
 *
 * @author zhaofuqiang
 * @date 2019/10/7 21:05
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
  private OrderBean k = new OrderBean();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");
    // 封装对象
    k.setOrderId(split[0]);
    k.setProductId(split[1]);
    k.setPrice(Double.parseDouble(split[2]));
    context.write(k, NullWritable.get());
  }
}

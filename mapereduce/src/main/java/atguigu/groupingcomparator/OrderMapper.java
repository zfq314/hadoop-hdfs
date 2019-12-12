package atguigu.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {
    private  OrderBean order = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //进行
        String[] split = value.toString().split("\t");
        order.setOrderId(split[0]);
        order.setProductId(split[1]);
        order.setPrice(Double.parseDouble(split[2]));
        context.write(order,NullWritable.get());
    }
}

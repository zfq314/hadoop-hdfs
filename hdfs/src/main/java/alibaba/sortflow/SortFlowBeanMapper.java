package alibaba.sortflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortFlowBeanMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
  private FlowBean k = new FlowBean();
  private Text v = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String string = value.toString();
    String[] split = string.split("\t");

    v.set(split[0]);
    long upFlow = Long.parseLong(split[1]);
    long downFlow = Long.parseLong(split[2]);
    k.set(upFlow, downFlow);
    context.write(k, v);
  }
}

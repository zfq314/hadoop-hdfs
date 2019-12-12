package atguigu.writablecomparable1;

import atguigu.writablecomparable.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComMapper extends Mapper<LongWritable,Text,FlowBean,Text> {;
    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        phone.set(split[0]);
        long up = Long.parseLong(split[1]);
        long down  = Long.parseLong(split[2]);

        flow.set(up,down);
        context.write(flow,phone);
    }
}

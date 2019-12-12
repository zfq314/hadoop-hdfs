package atguigu.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMap extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] files = value.toString().split("\t");
        phone.set(files[1]);
        //封装流量
        long upFlow = Long.parseLong(files[files.length - 3]);
        long downFlow = Long.parseLong(files[files.length - 2]);
        flowBean.set(upFlow, downFlow);
        context.write(phone, flowBean);
    }
}

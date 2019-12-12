package atguigu.mapflow;

import atguigu.flow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapFlowMap extends Mapper<LongWritable,Text,Text,FlowBean> {
    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();
    /**
     *
     * Map方法是我们MapTask的核心逻辑，对于每组输入的Key Value都执行一次。
     * @param key 行的偏移量
     * @param value 行的内容
     * @param context 任务本身
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] files = value.toString().split("\t");
        //获取电话号码作为key
        phone.set(files[1]);
        //获取流量上行和下行
        long upFlow = Long.parseLong(files[files.length-3]);
        //获取流量的下行
        long downFlow = Long.parseLong(files[files.length-2]);
        flowBean.set(upFlow,downFlow);
        //将数据写出
        context.write(phone,flowBean);

    }
}

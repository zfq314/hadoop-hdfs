package atguigu.mapflow;


import atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MapFlowReduce extends Reducer<Text, FlowBean,Text,FlowBean> {

    private  FlowBean flowBean = new FlowBean();

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
       //上行流量
        long upFlowSum = 0;
        //下行流量
        long downFlowSum = 0;
        for (FlowBean value : values) {
            upFlowSum = value.getUpFlow();
            downFlowSum = value.getDownFlow();
        }
        flowBean.set(upFlowSum,downFlowSum);
        context.write(key,flowBean);
    }
}

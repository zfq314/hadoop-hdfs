package atguigu.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean result = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //上行流量
        long sumUpFlow = 0;
        //下行流量
        long sumDownFlow = 0;
        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        result.set(sumUpFlow, sumDownFlow);
        context.write(key, result);

    }
}

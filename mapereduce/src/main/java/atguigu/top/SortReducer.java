package atguigu.top;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    private TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            FlowBean flowBean = new FlowBean();
            flowBean.set(key.getUpFlow(), key.getDownFlow());
            //map中添加元素
            treeMap.put(flowBean, new Text(value));

            //限制元素的数量
            if (treeMap.size() > 10) {
                treeMap.remove(treeMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //遍历数组元素
        Iterator<FlowBean> iterator = treeMap.keySet().iterator();
        while (iterator.hasNext()) {
            FlowBean bean = iterator.next();
            context.write(new Text(treeMap.get(bean)), bean);
        }
    }
}

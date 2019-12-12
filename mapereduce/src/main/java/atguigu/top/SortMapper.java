package atguigu.top;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    private TreeMap<FlowBean, Text> treeMap = new TreeMap<>();
    private FlowBean bean;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        bean = new FlowBean();
        Text text = new Text();

        //获取一行
        String line = value.toString();
        String[] split = line.split("\t");

        //封装数据
        String phone = split[0];
        long upFlow = Long.parseLong(split[1]);
        System.out.println(upFlow);
        long downFlow = Long.parseLong(split[2]);
        System.out.println(downFlow);
        long sumFlow = Long.parseLong(split[3]);
        System.out.println(sumFlow);
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        bean.setSumFlow(sumFlow);
        text.set(phone);
        //向map中添加数据
        treeMap.put(bean, text);
        //排序,限制流量，如果超过十条就删除流量最小的一条数据
        if (treeMap.size() > 10) {
            //treeMap.remove(treeMap.firstKey());
            treeMap.remove(treeMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //遍历集合并输出
        Iterator<FlowBean> iterator = treeMap.keySet().iterator();
        while (iterator.hasNext()) {
            FlowBean k = iterator.next();
            context.write(k, treeMap.get(k));
        }

    }
}

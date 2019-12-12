package atguigu.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //获取第一行
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getPname();
        //遍历所有的行，设置pname
        while (iterator.hasNext()){
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }


    }
}

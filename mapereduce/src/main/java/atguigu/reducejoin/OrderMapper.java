package atguigu.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

    private  OrderBean order = new OrderBean();
    private  String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //先获取名字
        String[] split = value.toString().split("\t");
        //开始判断属于那个文件
        if (filename.equals("order.txt")){
            //对象的封装
            order.setOrdeId(split[0]);
            order.setPid(split[1]);
            order.setAmout(Integer.parseInt(split[2]));
            order.setPname("");
        }else{
            order.setOrdeId("");
            order.setPid(split[0]);
            order.setAmout(0);
            order.setPname(split[1]);
        }
        context.write(order,NullWritable.get());
    }
}

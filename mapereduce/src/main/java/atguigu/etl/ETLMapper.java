package atguigu.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text text = new Text();
    private Counter total;
    private Counter pass;
    private Counter fail;

    /**
     * 设置数据清洗的过滤器
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        total = context.getCounter("ETL", "total");
        pass = context.getCounter("ETL", "pass");
        fail = context.getCounter("ETL", "fail");
    }

    /**
     * 处理数据的输入
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //处理清洗的数据
        String[] split = value.toString().split(" ");
        //判断split的长度不能小于11，如果长度小于11就是不合法的
        total.increment(1);
        if (split.length > 11) {
            pass.increment(1);
            text.set(value);
            context.write(text, NullWritable.get());
        } else {
            fail.increment(1);
        }
    }
}

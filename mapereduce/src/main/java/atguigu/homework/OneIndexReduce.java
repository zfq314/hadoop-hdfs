package atguigu.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneIndexReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable intWritable = new IntWritable();

    /**
     * 处理的核心逻辑
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        intWritable.set(sum);
        context.write(key, intWritable);
    }
}

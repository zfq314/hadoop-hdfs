package atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    /**
     * ReduceTask的核心计算逻辑
     * @param key 一组的单词
     * @param values 这个单词的好多1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //做累加
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        result.set(sum);

        context.write(key, result);
    }
}

package atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    /**
     * Map方法是我们MapTask的核心逻辑，对于每组输入的Key Value都执行一次。
     * @param key 行的偏移量
     * @param value 行的内容
     * @param context 人物本身
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 将一行内容切分为一组单词
        String[] words = value.toString().split(" ");

        //2. 将单词以（单词，1）的形式输出
        for (String word : words) {
            this.word.set(word);

            context.write(this.word, this.one);

        }
    }
}

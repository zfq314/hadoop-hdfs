package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(" ");

        for (String s : split) {
            k.set(s);
            context.write(k, v);
        }

    }
}

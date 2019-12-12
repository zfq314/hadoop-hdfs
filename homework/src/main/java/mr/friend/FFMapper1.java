package mr.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    //被关注的人
    private Text k = new Text();
    //关注别人的人
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(":");
        v.set(split[0]);
        String[] other = split[1].split(",");
        for (String s : other) {
            k.set(s);
            context.write(k, v);
        }
    }
}

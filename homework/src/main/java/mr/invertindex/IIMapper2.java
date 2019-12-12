package mr.invertindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IIMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        k.set(split[0]);
        v.set(split[1] + "-->" + split[2]);
        context.write(k, v);
    }
}

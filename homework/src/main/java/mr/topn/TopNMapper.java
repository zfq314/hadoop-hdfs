package mr.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
    private FlowBean[] flow = new FlowBean[11];

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < flow.length; i++) {
            flow[i] = new FlowBean();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        flow[10].setPhone(split[0]);
        flow[10].set(Long.parseLong(split[1]), Long.parseLong(split[2]));
        Arrays.sort(flow);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            context.write(flow[i], NullWritable.get());
        }
    }
}

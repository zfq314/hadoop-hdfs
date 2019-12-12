package mr.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FFReducer2 extends Reducer<Text, Text, Text, Text> {
    private Text v = new Text();
    private StringBuilder sb = new StringBuilder();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.setLength(0);
        for (Text value : values) {
            sb.append(value.toString()).append(",");
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}

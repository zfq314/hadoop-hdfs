package atguigu.homework;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TwoIndexReduce extends Reducer<Text, Text, Text, Text> {

    private Text text = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        //拼接
        for (Text value : values) {
            sb.append(value.toString().replace("\t", "-->") + "\t");
        }
        text.set(sb.toString());
        context.write(key, text);
    }
}

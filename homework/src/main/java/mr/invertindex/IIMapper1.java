package mr.invertindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IIMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片信息

        FileSplit split = (FileSplit) context.getInputSplit();
        //获取文件名字
        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行的数据
        String[] split = value.toString().split(" ");
        for (String s : split) {
            k.set(s + "\t" + filename);
            context.write(k, v);
        }

    }
}

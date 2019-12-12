package atguigu.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text text = new Text();
    private IntWritable v = new IntWritable();
    private String name;

    /**
     * 获取名字的长度
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件的切片信息
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //开始处理文件
        //读取一行文件
        String line = value.toString();
        //切割
        String[] split = line.split(" ");
        //拼接
        for (String files : split) {
            text.set(files + "--" + name);
            v.set(1);
            context.write(text, v);
        }

    }
}

package atguigu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //生成一个job实例
        Job job = Job.getInstance(new Configuration(), "MyWc");
        //类的路径
        job.setJarByClass(WcDriver.class);
        //设置Mapper和Reduce
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReduce.class);
        //map的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //文件最终的输入和输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

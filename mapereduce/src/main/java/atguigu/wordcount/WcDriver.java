package atguigu.wordcount;

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
        Configuration configuration = new Configuration();
        //1. 生成一个job实例
        Job job = Job.getInstance(configuration, "MyWC");
        configuration.set("mapred.job.queue.name", "hive");
        //设置类路径
        job.setJarByClass(WcDriver.class);
       /* // 开启 map 端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
*/
        // 设置 map 端输出压缩方式


        //2. 设置这个job的Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //2.5 设置Mapper和Reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WcReducer.class);

        // 设置 reduce 端输出压缩开启
        //FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        // FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        //3. 设置程序的输入输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //4. 提交任务
        boolean b = job.waitForCompletion(true);


        System.exit(b ? 0 : 1);
    }
}

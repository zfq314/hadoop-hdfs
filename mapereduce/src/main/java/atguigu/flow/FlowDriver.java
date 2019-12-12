package atguigu.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //实例化任务
        Job job = Job.getInstance(new Configuration());
        //调用任务
        job.setJarByClass(FlowDriver.class);
        //Map
        job.setMapperClass(FlowMap.class);
        job.setReducerClass(FlowReducer.class);
        //job.setInputFormatClass(CombineFileInputFormat.class);
        //map的路径
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //reduce的路径
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //文件的路径
        FileInputFormat.setInputPaths(job, new Path("d:/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/outpupt"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

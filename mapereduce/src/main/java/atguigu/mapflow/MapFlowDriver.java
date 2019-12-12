package atguigu.mapflow;
import atguigu.flow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapFlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //实例化任务
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MapFlowDriver.class);

        //获取Map类
        job.setMapperClass(MapFlowMap.class);
        //获取Reduce类
        job.setReducerClass(MapFlowReduce.class);
        //map的输出类型
        job.setMapOutputKeyClass(Text.class);
        //输出类型
        job.setMapOutputValueClass(FlowBean.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        //reduce的处理结果输出和输入
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path("d:/input"));
        FileOutputFormat.setOutputPath(job,new Path("d:/newOutput"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

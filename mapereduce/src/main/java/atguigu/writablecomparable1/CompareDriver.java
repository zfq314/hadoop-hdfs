package atguigu.writablecomparable1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CompareDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //实例化任务
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(CompareDriver .class);

       



        FileInputFormat.setInputPaths(job,new Path("d:/input"));
        FileOutputFormat.setOutputPath(job,new Path("d:Output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

}

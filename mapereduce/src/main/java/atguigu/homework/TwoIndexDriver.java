package atguigu.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TwoIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TwoIndexDriver.class);

        job.setMapperClass(TwoIndexMapper.class);
        job.setReducerClass(TwoIndexReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("d:/output"));
        FileOutputFormat.setOutputPath(job, new Path("d:/newOutput"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}

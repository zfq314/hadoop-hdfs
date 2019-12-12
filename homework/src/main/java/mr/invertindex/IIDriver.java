package mr.invertindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IIDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(IIDriver.class);

        job.setMapperClass(IIMapper1.class);
        job.setReducerClass(IIReduce1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("d:/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/iioutput"));
        boolean b = job.waitForCompletion(true);
        if (b) {
            Job job1 = Job.getInstance(new Configuration());
            job1.setJarByClass(IIDriver.class);

            job1.setMapperClass(IIMapper2.class);
            job1.setReducerClass(IIReduce2.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job1, new Path("d:/iioutput"));
            FileOutputFormat.setOutputPath(job1, new Path("d:/iiiiOutput"));
            boolean b1 = job1.waitForCompletion(true);
            System.exit(b1 ? 0 : 1);
        }

    }
}

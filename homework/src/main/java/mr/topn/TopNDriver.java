package mr.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopNDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TopNDriver.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(TopNComparator.class);

        FileInputFormat.setInputPaths(job, new Path("d:/test"));
        FileOutputFormat.setOutputPath(job, new Path("d:/outputTest"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

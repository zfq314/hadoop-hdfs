package atguigu.writablecomparable2;

import atguigu.writablecomparable.CompareMapper;
import atguigu.writablecomparable.CompareReducer;
import atguigu.writablecomparable.FlowBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionCompDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(PartitionCompDriver.class);

        job.setMapperClass(CompareMapper.class);
        job.setReducerClass(CompareReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job,new Path("d:/input"));
        FileOutputFormat.setOutputPath(job,new Path("d:/newOutput"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 :1);


    }
}

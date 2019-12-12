package atguigu.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TwoShareFriendsDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TwoShareFriendsDriver.class);

        job.setMapperClass(TwoShareFriendsMapper.class);
        job.setReducerClass(TwoShareFriendsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("d:/friendOutput"));
        FileOutputFormat.setOutputPath(job, new Path("d:/friendOutputNew"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

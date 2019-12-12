package mr.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FFDriver {
  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(FFDriver.class);

    job.setMapperClass(FFMapper1.class);
    job.setReducerClass(FFReducer1.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputValueClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, new Path("d:/friend"));
    FileOutputFormat.setOutputPath(job, new Path("d:/oldFriend"));
    boolean b = job.waitForCompletion(true);
    if (b) {
      Job job1 = Job.getInstance(new Configuration());
      job1.setJarByClass(FFDriver.class);
      job1.setMapperClass(FFMapper2.class);
      job1.setReducerClass(FFReducer2.class);

      job1.setMapOutputKeyClass(Text.class);
      job1.setMapOutputValueClass(Text.class);

      job1.setOutputValueClass(Text.class);
      job1.setOutputValueClass(Text.class);

      FileInputFormat.setInputPaths(job1, new Path("d:/oldFriend"));
      FileOutputFormat.setOutputPath(job1, new Path("d:/Fri_end"));
      boolean b1 = job1.waitForCompletion(true);
      System.exit(b1 ? 0 : 1);
    }
  }
}

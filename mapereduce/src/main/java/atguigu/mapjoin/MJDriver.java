package atguigu.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MJDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MJDriver.class);

        job.setMapperClass(MJMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置reduceTask的数量为零，就是不用
        job.setNumReduceTasks(0);

        //添加缓存文件
        job.addCacheFile(new URI("file:///d:/input/pd.txt"));

        FileInputFormat.setInputPaths(job,new Path("d:/input/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:/output"));
        //提交任务
        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}

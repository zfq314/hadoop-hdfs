package atguigu.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable,Text> {
    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public void initialize(TaskAttemptContext job) throws IOException {
        //获取文件的配置
        Configuration configuration = job.getConfiguration();
        //获取文件的系统HDFSc系统
        FileSystem fileSystem = FileSystem.get(configuration);
        //得到设置的路径
        String creDir = configuration.get(FileOutputFormat.OUTDIR);
        atguigu = fileSystem.create(new Path(creDir + "/atguigu.log"));
        other  = fileSystem.create(new Path(creDir + "/other.log"));
    }



    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
      String line = value.toString() + "\n";
        if (line.contains("atguigu")) {
            atguigu.write(line.getBytes());
        }else {

            other.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}

package atguigu.customize;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CustomizeFileInputFormat extends FileInputFormat {
  /**
   * 决定一个文件是否切分
   *
   * @param context
   * @param filename
   * @return
   */
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  /**
   * 自定义的处理操作 设置具体的处理操作，返回recoredReader
   *
   * @param split
   * @param context
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new MyRecordReader();
  }
}

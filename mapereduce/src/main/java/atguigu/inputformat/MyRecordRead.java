package atguigu.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordRead extends RecordReader<Text,BytesWritable> {

    //文件是否读取完毕
    private  boolean isReader = false;
    //读取kv值
    private  Text key = new Text();
    private  BytesWritable value = new BytesWritable();

    //获取文件系统和文件流
    private FSDataInputStream inputStream;

    private FileSplit fs;
    /**
     *初始化方法，框架在数据使用前调用一次
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //通过切边获取文件的路径
         fs = (FileSplit) split;
        Path path = fs.getPath();
        //获取文件的系统
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        //开流
        inputStream = fileSystem.open(path);
    }

    /**
     *尝试读取下一组KV
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //开始读取文件
        if (isReader) {
            return false;
        }else {
             //开始读取文件
            //读取key
            Path path = fs.getPath();
            key.set(path.toString());
            //读取value
            byte [] bytes = new byte[(int) fs.getLength()];

            int read = inputStream.read(bytes);

            value.set(bytes,0,read);

            isReader = true;
            return isReader;
        }
    }

    /**
     *读这个文件的内容
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     *获取这个行读取的内容
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 读取的进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        //1读取完毕，还没读取
        return isReader ? 1 : 0;
    }

    /**
     * 关闭流的操作
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}

package atguigu.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    private Text text = new Text();

    private Map<String,String> map = new HashMap<String, String>();

    /**
     * 缓存pd.txt进pMap
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {


        //获取缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        //开流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFiles[0]));
        //对流进行封装
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        //逐行读取
        String line;
        while(StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] split = line.split("\t");
            map.put(split[0],split[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    /**
     * 将读取的缓存文件中的pname直接替换
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //处理的文件是order.txt
        String[] split = value.toString().split("\t");
        String pname = map.get(split[1]);
        String values = split[0] + "\t" + pname + "\t" + split[2];

        text.set(values);
        context.write(text,NullWritable.get());
    }
}

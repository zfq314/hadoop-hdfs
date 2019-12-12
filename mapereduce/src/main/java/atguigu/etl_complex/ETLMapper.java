package atguigu.etl_complex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Counter total;
    private Counter pass;
    private Counter fail;

    private LogBean logBean = new LogBean();
    private Text text = new Text();

    /**
     * 设置计数器
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        total = context.getCounter(MyCounter.TOTAL);
        pass = context.getCounter(MyCounter.PASS);
        fail = context.getCounter(MyCounter.FAIL);
    }

    /**
     * 处理核心的逻辑
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        paseLog(line);
        total.increment(1);
        if (logBean.isValid()) {
            text.set(logBean.toString());
            context.write(text, NullWritable.get());
            pass.increment(1);
        } else {
            fail.increment(1);
        }
    }

    private void paseLog(String line) {
        String[] fields = line.split(" ");
        if (fields.length > 11) {
            // 2封装数据
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            logBean.setTime_local(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);

            if (fields.length > 12) {
                logBean.setHttp_user_agent(fields[11] + " " + fields[12]);
            } else {
                logBean.setHttp_user_agent(fields[11]);
            }
            // 大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400) {
                logBean.setValid(false);
            } else {
                logBean.setValid(true);
            }
        } else {
            logBean.setValid(false);
        }
    }
}

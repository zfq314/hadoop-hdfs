package mr.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    //两个有共同关注的人
    private Text k = new Text();
    //他们两个关注的人
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        v.set(split[0]);

        //关注了所有split[0]的人
        String[] persons = split[1].split(",");

        //两个相互组合
        for (int i = 0; i < persons.length; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                if (persons[i].compareTo(persons[j]) < 0) {
                    k.set(persons[i] + "-" + persons[j]);
                } else {
                    k.set(persons[j] + "-" + persons[i]);
                }
                context.write(k, v);
            }
        }
    }
}

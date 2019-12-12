package atguigu.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TwoShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        //友人
        String oneself = split[0];
        //自己
        String[] friend_person = split[1].split(",");
        Arrays.sort(friend_person);
        for (int i = 0; i < friend_person.length - 1; i++) {
            for (int j = i + 1; j < friend_person.length; j++) {
                context.write(new Text(friend_person[i] + "-" + friend_person[j]), new Text(oneself));
            }
        }

    }
}

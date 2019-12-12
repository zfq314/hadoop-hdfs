package atguigu.partition;

import atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * TelephoneNumPartition
 *
 * @author zhaofuqiang
 * @date 2019/10/7 13:05
 */
public class TelephoneNumPartition extends Partitioner<Text, FlowBean> {
  @Override
  public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
    int partition = 5;
    String tel = text.toString().substring(0, 3);
    // 判断电话号码会进入那个区
    switch (tel) {
      case "135":
        partition = 0;
        break;
      case "136":
        partition = 1;
        break;
      case "137":
        partition = 2;
        break;
      case "138":
        partition = 3;
        break;
      default:
        partition = 4;
        break;
    }
    return partition;
  }
}

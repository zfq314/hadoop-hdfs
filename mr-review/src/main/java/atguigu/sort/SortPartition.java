package atguigu.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * SortPartition
 *
 * @author zhaofuqiang
 * @date 2019/10/7 15:14
 */
public class SortPartition extends Partitioner<SortFlowBean, Text> {
  @Override
  public int getPartition(SortFlowBean sortFlowBean, Text text, int numPartitions) {
    int partition = 5;
    String tel = text.toString().substring(0, 3);
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

package alibaba.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitionerPartitioner extends Partitioner<Text, FlowBean> {
  @Override
  public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
    String substring = text.toString().substring(0, 3);
    int partitions;
    switch (substring) {
        //
      case "136":
        partitions = 0;
        break;
      case "137":
        partitions = 1;
        break;
      case "138":
        partitions = 2;
        break;
      case "139":
        partitions = 3;
        break;
      default:
        partitions = 4;
    }

    return partitions;
  }
}

package atguigu.partitioner;


import atguigu.top.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<FlowBean,Text> {
    @Override
    public int getPartition(FlowBean bean,Text text, int numPartitions) {
        int result = 0;
        switch (text.toString().substring(0,3)){
            case "136":
                result = 0 ;
                break;
            case "137":
                result = 1;
                break;
            case "138":
                result = 2;
                break;
            case "139":
                result = 3;
                break;
                default:
                    result = 4;
                    break;
        }
        return result;
    }
}

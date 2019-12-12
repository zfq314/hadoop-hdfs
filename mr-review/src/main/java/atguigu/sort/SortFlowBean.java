package atguigu.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * SortFlowBean
 *
 * @author zhaofuqiang
 * @date 2019/10/7 14:31
 */
public class SortFlowBean implements WritableComparable<SortFlowBean> {
  private long upFlow;
  private long downFlow;
  private long sumFlow;

  // 反序列化时，需要反射调用空参构造函数，所以必须有
  public SortFlowBean() {
    super();
  }

  public SortFlowBean(long upFlow, long downFlow) {
    super();
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  public void set(long upFlow, long downFlow) {
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  public long getSumFlow() {
    return sumFlow;
  }

  public void setSumFlow(long sumFlow) {
    this.sumFlow = sumFlow;
  }

  public long getUpFlow() {
    return upFlow;
  }

  public void setUpFlow(long upFlow) {
    this.upFlow = upFlow;
  }

  public long getDownFlow() {
    return downFlow;
  }

  public void setDownFlow(long downFlow) {
    this.downFlow = downFlow;
  }

  /**
   * 自定义比较器全局排序
   *
   * @param o
   * @return
   */
  @Override
  public int compareTo(SortFlowBean o) {
    int result;
    // 按照总流量倒序排列
    if (sumFlow > o.getSumFlow()) {
      result = -1;
    } else if (sumFlow == o.getSumFlow()) {
      result = 0;
    } else {
      result = 1;
    }

    return result;
  }

  /**
   * 序列化方法
   *
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(upFlow);
    out.writeLong(downFlow);
    out.writeLong(sumFlow);
  }

  /**
   * 反序列化方法
   *
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    upFlow = in.readLong();
    downFlow = in.readLong();
    sumFlow = in.readLong();
  }

  @Override
  public String toString() {
    return upFlow + "\t" + downFlow + "\t" + sumFlow;
  }
}

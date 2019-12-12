package atguigu.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** 自定义的序列化对象，实现writerable */
public class FlowBean implements Writable {
  // 私有属性
  private long upFlow;
  private long downFlow;
  private long sumFlow;

  public FlowBean() {
    super();
  }

  public FlowBean(long upFlow, long downFlow) {
    this();
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  public void set(long upFlow, long downFlow) {
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  @Override
  public String toString() {
    return upFlow + "\t" + downFlow + "\t" + sumFlow;
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

  public long getSumFlow() {
    return sumFlow;
  }

  public void setSumFlow(long sumFlow) {
    this.sumFlow = sumFlow;
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
   * 反序列化 和序列化的顺序一致
   *
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.upFlow = in.readLong();
    this.downFlow = in.readLong();
    this.sumFlow = in.readLong();
  }
}

package atguigu.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * OrderComparator
 *
 * @author zhaofuqiang
 * @date 2019/10/7 21:01
 */
public class OrderComparator extends WritableComparator {
  public OrderComparator() {
    super(OrderBean.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    OrderBean oa = (OrderBean) a;
    OrderBean ob = (OrderBean) b;
    return oa.getOrderId().compareTo(ob.getOrderId());
  }
}

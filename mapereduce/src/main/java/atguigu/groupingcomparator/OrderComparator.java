package atguigu.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator {

    public OrderComparator() {
        super(OrderBean.class,true);
    }

    //构造器
    @Override

    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean  ob = (OrderBean) b;
        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}

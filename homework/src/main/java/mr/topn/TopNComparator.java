package mr.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNComparator extends WritableComparator {
    protected TopNComparator() {
        super(FlowBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}

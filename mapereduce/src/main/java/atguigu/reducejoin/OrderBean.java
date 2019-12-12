package atguigu.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable <OrderBean>{
    private String ordeId;
    private String pid;
    private int amout;
    private String pname;

    public String getOrdeId() {
        return ordeId;
    }

    public void setOrdeId(String ordeId) {
        this.ordeId = ordeId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmout() {
        return amout;
    }

    public void setAmout(int amout) {
        this.amout = amout;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }


    //定义排序规则
    @Override
    public int compareTo(OrderBean o) {
        int compare = o.pid.compareTo(this.pid);
        if (compare == 0){
            return o.pname.compareTo(this.pname);
        }
        return compare;
    }

    @Override
    public String toString() {
         return ordeId + "\t" + pname + "\t" + amout;
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
    out.writeUTF(ordeId);
    out.writeUTF(pid);
    out.writeInt(amout);
    out.writeUTF(pname);
    }
    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
    this.ordeId = in.readUTF();
    this.pid = in.readUTF();
    this.amout = in.readInt();
    this.pname = in.readUTF();
    }
}

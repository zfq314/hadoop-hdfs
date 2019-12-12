package alibaba.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;

/** hdfs客户端连接 */
public class HDFSClient {
  private Configuration configuration;
  private FileSystem fs;

  @Before
  public void before() throws IOException, InterruptedException {
    configuration = new Configuration();
    // configuration.set("dfs.replication", "5");
    fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), configuration, "atguigu");
  }

  @Test
  public void mkdir() throws IOException {
    boolean mkdirs = fs.mkdirs(new Path("/newModify"));
    System.out.println(mkdirs);
  }

  @Test
  // 上传文件
  public void copyFromLocalFile() throws IOException {
    fs.copyFromLocalFile(new Path("d:/input/a.txt"), new Path("/newModify"));
  }
  // 下载文件到本地
  @Test
  public void copyToLocalFile() throws IOException {
    fs.copyToLocalFile(new Path("/newModify/input"), new Path("e:/"));
  }

  @After
  public void after() throws IOException {
    fs.close();
  }
}

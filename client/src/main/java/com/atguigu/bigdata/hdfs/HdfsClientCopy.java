package com.atguigu.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class HdfsClientCopy {
    private FileSystem fs;
    private Configuration configuration;

    @Before
    public void before() throws IOException, InterruptedException {
        //获取文件系统
        configuration = new Configuration();
        //配置在集群上运行
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), configuration, "atguigu");
    }

    @After
    public void after() throws IOException {
        fs.close();
    }

    @Test
    public void mkdir() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/New2"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        System.out.println(mkdirs);
    }

    @Test
    public void cp() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/new.log"));
        FSDataOutputStream outputStream = fs.create(new Path("/copy.log"));
        IOUtils.copyBytes(inputStream, outputStream, 1024);
    }

    @Test
    public void setrepect() throws IOException {
        boolean b = fs.setReplication(new Path("/copy.log"), (short) 4);
        System.out.println(b);
    }

    @Test
    public void readFile() throws IOException {
        //读取流
        FSDataInputStream inputStream = fs.open(new Path("/copy.log"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String string;
        while ((string = bufferedReader.readLine()) != null) {
            System.out.println(string);
        }
        bufferedReader.close();
    }

    @Test
    public void fl() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                long offset = blockLocation.getOffset();
                String[] names = blockLocation.getNames();
                for (String name : names) {
                    System.out.println(name);
                    System.out.println(offset);
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }

            }
            System.out.println("=======================");
        }

    }
    //上传
    @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("H:\\BigData\\第三阶段\\01_尚硅谷大数据技术之hadoop\\2.资料\\01_jar包\\03_linux编译过的hadoop jar包\\"),new Path("/"));
    }
}
package com.atguigu.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    private  FileSystem fs;
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
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {

        //创建目录
        boolean mkdirs = fs.mkdirs(new Path("/testClient"));
        System.out.println(mkdirs);
        //关闭资源
        fs.close();
    }

    //HDFS文件上传
    @Test
    public void uploadFileHdfs() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "11");
        //上传文件
        fs.copyFromLocalFile(new Path("d:/MySQL5.6history.log"), new Path("/new123.log"));
        //关闭资源
        fs.close();
        System.out.println("over");
    }

    @Test
    public void testCopyToLocalFile() throws IOException {
        //下载文件到本地
        //执行下载的操作
        //四个参数，boolean delSrc 是否将原文件删除
        //下载路径
        //目录路径
        //是否开启文件校验
        fs.copyToLocalFile(false, new Path("/new.log"), new Path("e:/mysql.log"), true);
        //关闭资源
        fs.close();
    }

    //删除文件夹
    @Test
    public void delDir() throws IOException {
        boolean delete = fs.delete(new Path("/testClient"), true);
        System.out.println(delete);
        fs.close();
    }

    //修改文件名
    @Test
    public void reName() throws IOException {
        //修改文件名
        boolean rename = fs.rename(new Path("/new.log"), new Path("/mysql.log"));
        System.out.println(rename);
        fs.close();
    }

    //文件详情查看
    @Test
    public void testListFile() throws IOException {
        //获取文件的详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            //输出文件的详情
            //文件的名称
            String name = next.getPath().getName();
            System.out.println(name);
            //文件的长度
            long len = next.getLen();
            System.out.println(len);
            //文件的权限
            FsPermission permission = next.getPermission();
            System.out.println(permission);
            //文件分组
            String group = next.getGroup();
            System.out.println(group);
            //文件的拥有者
            String owner = next.getOwner();
            System.out.println(owner);
            //获取存储的块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            //遍历信息
            for (BlockLocation blockLocation : blockLocations) {
                //获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------------------");
        }
        fs.close();
    }

    //文件和文件夹的判断
    @Test
    public void testListStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            //判断是文件还是文件夹
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        fs.close();
    }
}

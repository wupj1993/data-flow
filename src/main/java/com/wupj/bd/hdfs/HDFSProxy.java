package com.wupj.bd.hdfs;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class HDFSProxy {


    private FileSystem fs = null;

    public static String default_address = "hdfs://uf001:9000/";
    public static String default_username = "root";
    private static HDFSProxy hdfsProxy;
    private Configuration conf;

    public static HDFSProxy getInstance() throws Exception {
        if (hdfsProxy == null) {
            synchronized (HDFSProxy.class) {
                if (hdfsProxy == null) {
                    hdfsProxy = new HDFSProxy();
                }
            }
        }
        return hdfsProxy;
    }

    private HDFSProxy() throws URISyntaxException, IOException, InterruptedException {
        fs = FileSystem.get(new URI(default_address), getConf(), default_username);
    }


    public boolean exists(String dir) throws Exception {
        return fs.exists(new Path(dir));
    }

    //列出指定目录下所有文件 包含目录
    public FileStatus[] listDir(String path) throws Exception {
        return fs.listStatus(new Path(path));

    }

    //列出指定目录下所有文件 不包含目录 非递归
    public List<String> listFile(String path, boolean recursive) throws Exception {
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(path), recursive);
        List<String> fileList = new ArrayList<>();
        while (itr.hasNext()) {
            LocatedFileStatus next = itr.next();
            System.out.println(next.toString());
            fileList.add(next.getPath().getName());
        }
        return fileList;
    }

    //列出指定目录下所有文件 不包含目录 递归 
    public List<String> listFile(String path) throws Exception {
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(path), false);
        List<String> fileList = new ArrayList<>();
        while (itr.hasNext()) {
            LocatedFileStatus next = itr.next();
            fileList.add(next.getPath().getName());
        }
        return fileList;
    }


    // 
    public void createFile(String path, byte[] contents) throws Exception {
        FSDataOutputStream f = fs.create(new Path(path));
        f.write(contents);
        f.close();
    }

    public void createFile(String filePath, String contents) throws Exception {
        createFile(filePath, contents.getBytes());
    }


    public void copyFromLocalFile(String src, String dst) throws Exception {
        fs.copyFromLocalFile(new Path(src), new Path(dst));
    }

    //mkdir
    public boolean mkdir(String dir) throws Exception {
        boolean isSuccess = fs.mkdirs(new Path(dir));
        return isSuccess;
    }

    //rm
    public boolean delete(String filePath, boolean recursive) throws Exception {
        boolean isSuccess = fs.delete(new Path(filePath), recursive);
        return isSuccess;
    }

    //rm -r
    public boolean delete(String filePath) throws Exception {
        boolean isSuccess = fs.delete(new Path(filePath), true);
        return isSuccess;
    }

    public void rename(String oldName, String newName) throws Exception {
        fs.rename(new Path(oldName), new Path(newName));
    }

    //
    public String readFile(String filePath) throws Exception {
        String content = null;
        InputStream in = null;
        ByteArrayOutputStream out = null;

        try {
            in = fs.open(new Path(filePath));
            out = new ByteArrayOutputStream(in.available());
            IOUtils.copyBytes(in, out, getConf());
            content = out.toString();
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

        return content;
    }

    public void moveToLocalFile(String srcPath, String targetPath) throws IOException {
        Path src = new Path(srcPath);
        Path target = new Path(targetPath);
        fs.moveToLocalFile(src, target);
    }


    private Configuration getConf() {
        if (conf == null) {
            // 假装 读取配置文件
            conf = new Configuration();
            conf.set("hadoop.home.dir", "D:\\hadoop-3.2.0\\");
        }
        return conf;
    }
}
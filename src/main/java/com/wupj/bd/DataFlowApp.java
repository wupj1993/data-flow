package com.wupj.bd;

import com.wupj.bd.common.Constant;
import com.wupj.bd.common.SqoopCallback;
import com.wupj.bd.hdfs.HDFSProxy;
import com.wupj.bd.sqoop.Sqoop2Job;
import com.wupj.bd.utils.DateUtil;

import java.util.Date;
import java.util.List;

/**
 * Hello world!
 */
public class DataFlowApp {
    public static void main(String[] args) throws Exception {
        //1. move data dir's files to data-bak
        String targetDir = DateUtil.DateToString(new Date(), DateUtil.DateStyle.YYYY_MM_DD);
        HDFSProxy.getInstance().mkdir(Constant.DATA_HDFS_BAK_PATH + "/" + targetDir);
        List<String> fileList = HDFSProxy.getInstance().listFile(Constant.HDFS_HOST_PORT + Constant.DATA_HDFS_PATH + "/");
        System.out.println(fileList.size());
        if (fileList.size() > 0) {
            fileList.forEach(fileName -> {
                try {
                    HDFSProxy.getInstance().rename(Constant.HDFS_HOST_PORT + Constant.DATA_HDFS_PATH + "/" + fileName,
                            Constant.HDFS_HOST_PORT + Constant.DATA_HDFS_BAK_PATH + "/" + targetDir + "/" + fileName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        //2.执行sqoop数据抽取

    Sqoop2Job.startJob(new SqoopCallback() {
            @Override
            public void succes() {
                // 传入第二个任务
                System.out.println("sqoop success ,It's will start Phoenix ");
               /* try {
                    PhoenixJob.startPhoenix();
                    System.out.println("phoenix over");
                    SparkJob.metricJob();
                    System.out.println("spark over");
                } catch (Exception e) {
                    System.out.println("Phoenix Fail ");
                    throw new RuntimeException(e);
                }*/
                // start spark

            }

            @Override
            public void fail(String msg) {
                System.out.println("失败了:" + msg);
            }

            @Override
            public void update(double progress) {
                System.out.println("进度：" + progress);
            }
        });



    }
}

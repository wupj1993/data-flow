package com.wupj.bd.sqoop;

import com.wupj.bd.common.Constant;
import com.wupj.bd.common.SqoopCallback;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.model.*;
import org.apache.sqoop.validation.Status;

import java.util.LinkedList;

/**
 * 〈〉
 *
 * @author wupeiji
 * @create 2019/3/21
 * @since 1.0.0
 */
public class Sqoop2Job {
    private static final String SQOOP_URL = "http://uf001:12000/sqoop/";
    private static final String MYSQL_LINK = "data-flow-mysql-link";
    private static final String HDFS_LINK = "data-flow-hdfs-link";
    private static final String MYSQL_2_HDFS_JOB = "mysql-2-hdfs-job";
    private static SqoopClient sqoopClient;

    public static void main(String[] args) {
        getSqoopClient().deleteAllLinks();
        getSqoopClient().deleteAllJobs();
        String mysqlLink = createMysqlLink();
        String createHdfsLink = createHdfsLink();
        String mysql2HDFSJob = createMysql2HDFSJob();
        System.out.println("createMysqlLink:" + mysqlLink);
        System.out.println("createHdfsLink:" + createHdfsLink);
        System.out.println("mysql2HDFSJob:" + mysql2HDFSJob);
    }

    private static String createMysqlLink() {
        String fromConnectorName = "generic-jdbc-connector";
        MLink fromLink = getSqoopClient().createLink(fromConnectorName);
        fromLink.setCreationUser("root");
        MLinkConfig fromLinkConfig = fromLink.getConnectorLinkConfig();
        fromLinkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://10.188.181.72:3306/sodog");
        fromLinkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
        fromLinkConfig.getStringInput("linkConfig.username").setValue("root");
        fromLinkConfig.getStringInput("linkConfig.password").setValue("YYTdb@2019");
        fromLink.getConnectorLinkConfig("dialect").getStringInput("dialect.identifierEnclose").setValue(" ");
        fromLink.setName(MYSQL_LINK);
        Status status = getSqoopClient().saveLink(fromLink);
        return status.name();
    }

    private static String createHdfsLink() {
        MLink hdfsLink = getSqoopClient().createLink("hdfs-connector");
        hdfsLink.setName(HDFS_LINK);
        hdfsLink.setCreationUser("root");
        MLinkConfig toLinkConfig = hdfsLink.getConnectorLinkConfig();
        toLinkConfig.getStringInput("linkConfig.uri").setValue("hdfs://uf001:9000/");
        Status status = getSqoopClient().saveLink(hdfsLink);
        return status.name();
    }

    private static String createMysql2HDFSJob() {
        MJob job = getSqoopClient().createJob(MYSQL_LINK, HDFS_LINK);
        job.setName(MYSQL_2_HDFS_JOB);
        job.setCreationUser("root");
        MConfigList jobConfig = job.getFromJobConfig();
        jobConfig.getStringInput("fromJobConfig.tableName").setValue("YYTERP_SCC_SALEINFO");
        jobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("CREATE_TS");
        jobConfig.getStringInput("incrementalRead.checkColumn").setValue("CREATE_TS");
        jobConfig.getStringInput("incrementalRead.lastValue").setValue("2018-11-11 11:11:11");
        LinkedList<String> columnList = new LinkedList<>();
        columnList.add("PK_SCC_SALEINFO");
        columnList.add("CYEAR");
        columnList.add("CMONTH");
        columnList.add("DSALEDATE");
        columnList.add("VINSTITUTIONCODE");
        columnList.add("CCUSTOMERCODE");
        columnList.add("CINVENTORYCODE");
        columnList.add("VBRAND");
        columnList.add("VINVLEVEL");
        columnList.add("VCITYCODE");
        columnList.add("VPROVINCECODE");
        columnList.add("NNUM");
        columnList.add("NMNY");
        columnList.add("TS");
        columnList.add("CREATE_TS");
        jobConfig.getListInput("fromJobConfig.columnList").setValue(columnList);
        MConfigList toConfig = job.getToJobConfig();
        toConfig.getStringInput("toJobConfig.outputDirectory").setValue(Constant.DATA_HDFS_PATH);
        toConfig.getStringInput("toJobConfig.nullValue").setValue("''");
        toConfig.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE");
        toConfig.getBooleanInput("toJobConfig.overrideNullValue").setValue(true);
        toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true);

        MDriverConfig driverConfig = job.getDriverConfig();
        driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);
        driverConfig.getIntegerInput("throttlingConfig.numLoaders").setValue(1);
        Status status = getSqoopClient().saveJob(job);
        return status.name();
    }

    public static boolean startJob(final SqoopCallback sqoopCallback) throws InterruptedException {
        MStringInput stringInput = getSqoopClient().getJob(MYSQL_2_HDFS_JOB).getToJobConfig().getStringInput("toJobConfig.outputDirectory");
        System.out.println("out put data"+stringInput.getValue());
        MSubmission submission = getSqoopClient().startJob(MYSQL_2_HDFS_JOB, new SubmissionCallback() {
            @Override
            public void submitted(MSubmission mSubmission) {
                System.out.println("submitted"+mSubmission.getStatus());
            }

            @Override
            public void updated(MSubmission mSubmission) {
                System.out.println("updated"+mSubmission.getProgress());
            }

            @Override
            public void finished(MSubmission mSubmission) {
                String name = mSubmission.getStatus().name();
                System.out.println("finished:"+name);
                sqoopCallback.succes();
            }
        },3000);
        System.out.println(submission.getStatus());
        return true;
  /*      long l = System.currentTimeMillis();
        while (submission.getStatus().isRunning() && submission.getProgress() != -1) {
            System.out.println("任务进度 : " + String.format("%.2f %%", submission.getProgress() * 100));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("任务异常：" + e);
                return false;
            }
        }
        Counters counters = submission.getCounters();
        if (counters != null) {
            System.out.println("计数器:");
            for (CounterGroup group : counters) {
                System.out.print("\t");
                System.out.println(group.getName());
                for (Counter counter : group) {
                    System.out.print("\t\t");
                    System.out.print(counter.getName());
                    System.out.print(": ");
                    System.out.println(counter.getValue());
                }
            }
        }
        if (submission.getError() != null) {
            System.out.println(submission.getError().getErrorSummary());
            System.out.println("任务执行失败,费时" +submission.getError().getErrorDetails());
            return false;
        } else {
            System.out.println("任务执行成功,费时" + (System.currentTimeMillis() - l));
            return true;
        }*/
    }

    private static SqoopClient getSqoopClient() {
        if (sqoopClient == null) {
            synchronized (Sqoop2Job.class) {
                if (sqoopClient == null) {
                    sqoopClient = new SqoopClient(SQOOP_URL);
                }
            }
        }
        return sqoopClient;
    }
}

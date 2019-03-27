package com.wupj.bd.phoenix;

import com.wupj.bd.hdfs.HDFSProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.mapreduce.CsvBulkLoadTool;

import java.util.List;

/**
 * 〈Phoenix任务定义〉
 *
 * @author wupeiji
 * @create 2019/3/22
 * @since 1.0.0
 */
public class PhoenixJob {
    /**
     * 启动Phoenix任务
     */
    public static void startPhoenix() throws Exception {
        System.out.println("[phoenix]-----starting");
    // 第一步读取文件目录下的文件
        List<String> strings = HDFSProxy.getInstance().listFile("/uf/sqoop/data");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        //指定resourcemanager的主机名
        conf.set("yarn.resourcemanager.hostname", "uf001");
        conf.set("fs.defaultFS","hdfs://uf001:9000/");
        conf.set("hbase.zookeeper.quorum", "uf001:2181");
        conf.set("hadoop.registry.zk.quorum", "uf001:2181");
        conf.set("yarn.application.classpath",
                "{{HADOOP_HOME}}/share/hadoop/common/*,{{HADOOP_HOME}}/share/hadoop/common/lib/*,"
                        + " {{HADOOP_HOME}}/share/hadoop/hdfs/*,{{HADOOP_HOME}}/share/hadoop/hdfs/lib/*,"
                        + "{{HADOOP_HOME}}/share/hadoop/mapreduce/*,{{HADOOP_HOME}}/share/hadoop/mapreduce/lib/*,"
                        + "{{HADOOP_HOME}}/share/hadoop/yarn/*,{{HADOOP_HOME}}/share/hadoop/yarn/lib/*,{{HADOOP_HOME}}/extrajar/*");
        Job job = Job.getInstance(conf);
        job.setJarByClass(CsvBulkLoadTool.class);
        job.setJar("hdfs://uf001:9000/uf/jar/phoenix-5.0.0-HBase-2.0-client.jar");
        job.setJobName("phoenix-import");
        CsvBulkLoadTool csvBulkLoadTool = new CsvBulkLoadTool();
        csvBulkLoadTool.setConf(conf);
        strings.forEach(data-> {
            System.out.println("file-name"+data);
            // -q "'" 因为sqoop2写入使用单引号引起来的
            String[] strings1 = {"--table","YYTERP_SCC_SALEINFO_TEST","--input","hdfs://uf001:9000/uf/sqoop/data/"+data,"-z","uf001:2181","-q","'"};
            long start=System.currentTimeMillis();
            int run = 0;
            try {
                run = ToolRunner.run(csvBulkLoadTool, strings1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("file"+data+(run==1?"success":"fail")+" time :"+(System.currentTimeMillis()-start));
        });


    }

    public static void main(String[] args) throws Exception {
        startPhoenix();
    }
}

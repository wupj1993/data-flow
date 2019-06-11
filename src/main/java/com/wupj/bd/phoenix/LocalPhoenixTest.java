package com.wupj.bd.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.phoenix.mapreduce.CsvBulkLoadTool;
import org.apache.phoenix.mapreduce.CsvToKeyValueMapper;
import org.apache.phoenix.mapreduce.FormatToKeyValueReducer;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;

import java.util.UUID;

/**
 * 〈本地址Phoenix数据大量导入测试〉
 *
 * @author wupeiji
 * @date 2019/5/16 11:28
 * @since 1.0.0
 */
public class LocalPhoenixTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
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
        String[] strings1 = {"--table","MY_YYT","--input","hdfs://uf001:9000/BDF/TEST/usermsg.csv","-z","uf001:2181","-q","'"};
        FileInputFormat.addInputPaths(job, "hdfs://uf001:9000/BDF/TEST/usermsg.csv");
        FileOutputFormat.setOutputPath(job,   new Path("/tmp/" + UUID.randomUUID()));
    //    Configuration hbaseConf = new Configuration();
      //  conf.set("mapreduce.framework.name", "yarn");
        //指定resourcemanager的主机名

        job.setMapperClass(CsvToKeyValueMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(TableRowkeyPair.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);
        job.setOutputKeyClass(TableRowkeyPair.class);
        job.setOutputValueClass(KeyValue.class);
        job.setReducerClass(FormatToKeyValueReducer.class);
        final boolean b = job.waitForCompletion(true);
        System.out.println(b);
    }

}

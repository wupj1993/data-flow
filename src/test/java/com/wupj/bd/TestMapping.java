package com.wupj.bd;

import org.apache.spark.util.CollectionsUtils;

import java.util.*;

/**
 * 〈测试映射〉
 *
 * @author wupeiji
 * @date 2019/5/14 14:31
 * @since 1.0.0
 */
public class TestMapping {
    public static void main(String[] args) {
        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("pk", "id");
        fieldMapping.put("name", "username");
        fieldMapping.put("age", "userage");
        Map<String, Object> valueMapping = new HashMap<>();
        valueMapping.put("pk", "0000000000001");
        valueMapping.put("name", "zhangsan");
        valueMapping.put("age", 12);

        List<String> ori = new ArrayList<>();
        List<String> target = new ArrayList<>();

        fieldMapping.forEach((k, v) -> {
            ori.add(k);
            target.add(v);
        });
        for (int i = 0; i < ori.size(); i++) {
            System.out.println("映射关系:" + ori.get(i) + ":" + target.get(0));
        }
        // 拼装sql参数
        StringBuffer sql = new StringBuffer("upsert  into test ");
        String fields = String.join(",", target);
        sql.append(fields);
        sql.append(" values ");
        for (int i = 0; i < ori.size(); i++) {
            sql.append("'").append(valueMapping.get(ori.get(i).trim())).append("'");
            if (i < ori.size() - 1) {
                sql.append(",");
            }
        }
        System.out.println(sql.toString());
    }
}

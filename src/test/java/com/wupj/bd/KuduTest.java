package com.wupj.bd;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 〈kudu test〉
 *
 * @author wupeiji
 * @date 2019/5/10 15:36
 * @since 1.0.0
 */
public class KuduTest {
    private static final Double DEFAULT_DOUBLE = 12.345;
    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "192.168.254.10:7051");

    private KuduClient client;

    @Before
    public void init() {
        client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        System.out.println("创建Kudu客户端");
    }

    @Test
    public void createTable() throws KuduException {
        /**
         * 字段定义
         */
        List<ColumnSchema> columns = new ArrayList<>(10);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("username", Type.STRING)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32)
                .build());
//        Type decimal = Type.DECIMAL; 有点问题，不知道怎么设置精度
        columns.add(new ColumnSchema.ColumnSchemaBuilder("salary", Type.DOUBLE)
                .build());
        Schema schema = new Schema(columns);
        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("id");
        int numBuckets = 10;
        cto.addHashPartitions(hashKeys, numBuckets);
        client.createTable("usermsg", schema, cto);
        System.out.println("建表成功");
    }

    @Test
    public void insertData() throws KuduException {
        KuduTable table = client.openTable("usermsg");
        KuduSession session = client.newSession();
        for (int i = 0; i < 1000; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", i);
            row.addString("username", "Test_" + i);

            row.addInt("age", 1);
            row.addDouble("salary", Math.random());
            session.apply(insert);
        }
        session.flush();
        session.close();
        if (session.countPendingErrors() != 0) {
            System.out.println("errors inserting rows");
            org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
            org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
            int numErrs = Math.min(errs.length, 5);
            System.out.println("there were errors inserting rows to Kudu");
            System.out.println("the first few errors follow:");
            for (int i = 0; i < numErrs; i++) {
                System.out.println(errs[i]);
            }
            if (roStatus.isOverflowed()) {
                System.out.println("error buffer overflowed: some errors were discarded");
            }
            throw new RuntimeException("error inserting rows to Kudu");
        }
        System.out.println("插入数据结束");
    }


    @Test
    public void selectData() throws KuduException {
        KuduTable table = client.openTable("usermsg");
        final Schema schema = table.getSchema();
        KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("id"),
                KuduPredicate.ComparisonOp.GREATER_EQUAL,
                300);
        KuduScanner scanner = client.newScannerBuilder(table)
                .addPredicate(lowerPred).build();

        while (scanner.hasMoreRows()) {
            RowResultIterator rowResults = scanner.nextRows();
            System.out.println(rowResults.getNumRows());
            while (rowResults.hasNext()) {
                RowResult result = rowResults.next();
                int id = result.getInt("id");
                int age = result.getInt("age");
                double salary = result.getDouble("salary");
                String username = result.getString("username");
                System.out.println("id=" + id + ",age=" + age + ",salary=" + salary + ",username=" + username);
            }
        }
    }


    public void count() throws KuduException {
        KuduTable table = client.openTable("YYTERP_SCC_SALEINFO");
        KuduSession session = client.newSession();
        client.newScannerBuilder(table).build();
    }
}

package com.wupj.bd.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 〈〉
 *
 * @author wupeiji
 * @date 2019/5/14 16:45
 * @since 1.0.0
 */
public class JdbcTest {
    public static void main(String[] args) {

        StringBuilder sql = new StringBuilder("UPSERT INTO MY_YYT (\"pk\",\"name\",\"age\") values(?,?,?)");
        try (Connection connection = DriverManager.getConnection("jdbc:phoenix:uf001:2181")) {
            final PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
            for (int i = 0; i < 10; i++) {
                preparedStatement.setString(1, (i + 10) + "");
                preparedStatement.setString(2, "name" + i);
                preparedStatement.setInt(3, i);
                preparedStatement.addBatch();
            }
            final int[] ints = preparedStatement.executeBatch();
            System.out.println(ints);
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

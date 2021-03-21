package com.cjs.hadoopLearn.hiveLearn.hiveJDBCLearn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TEST_One {

    private static Connection conn = ConnectionLearn.getConnnection();
    private static PreparedStatement ps;
    private static ResultSet rs;

    public static void getAll(String tablename) {
        String sql = "select * from " + tablename;
        System.out.println(sql);
        try {
            ps = ConnectionLearn.prepare(conn, sql);
            rs = ps.executeQuery();
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    System.out.print(rs.getString(i));
                    System.out.print("\t\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        String tablename = "student";
        TEST_One.getAll(tablename);
    }


}

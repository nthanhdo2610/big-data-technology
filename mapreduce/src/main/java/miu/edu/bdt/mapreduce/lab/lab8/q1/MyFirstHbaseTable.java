package miu.edu.bdt.mapreduce.lab.lab8.q1;


import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyFirstHbaseTable {
    private static final String TABLE_NAME = "user";
    private static final String CF_PERSONAL = "personal_details";
    private static final String CF_PROFESSIONAL = "prof_details";

    public static void main(String... args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableDescriptor table = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(TABLE_NAME))
                    .setColumnFamilies(Lists.newArrayList(
                            ColumnFamilyDescriptorBuilder.of(CF_PERSONAL),
                            ColumnFamilyDescriptorBuilder.of(CF_PROFESSIONAL)))
                    .build();


            System.out.print("Creating table.... ");
            try {
                System.out.println("try to delete exist table.... ");
//                if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                    admin.disableTable(table.getTableName());
                    admin.deleteTable(table.getTableName());
//                }
            } catch (Exception exception){
                System.out.println("try to delete exist table error.... " + exception.getMessage());
            }
            admin.createTable(table);
            System.out.println("Done!");

            System.out.print("Inserting data...");
            Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
            List<Put> rows = new ArrayList<>();
            rows.add(putData("1", "John", "Boston", "Manager", 150000));
            rows.add(putData("2", "Mary", "New York", "Sr. Engineer", 130000));
            rows.add(putData("3", "Bob", "Fremont", "Jr. Engineer", 90000));
            users.put(rows);
            users.close();
            System.out.println("Done!");
        }
    }

    private static final String COL_NAME = "Name";
    private static final String COL_CITY = "City";
    private static final String COL_DESIGNATION = "Designation";
    private static final String COL_SALARY = "Salary";

    private static Put putData(String empId, String name, String city, String designation, Integer salary) {
        Put p = new Put(Bytes.toBytes(empId));
        p.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_NAME), Bytes.toBytes(name));
        p.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_CITY), Bytes.toBytes(city));
        p.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_DESIGNATION), Bytes.toBytes(designation));
        p.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_SALARY), Bytes.toBytes(String.format("%,d", salary)));
        return p;
    }
}

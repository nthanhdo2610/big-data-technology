package miu.edu.bdt.mapreduce.lab.lab8.q2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyFirstHbaseTable {
    private static final String TABLE_NAME = "user";
    private static final String CF_PERSONAL = "personal_details";
    private static final String CF_PROFESSIONAL = "prof_details";

    public static void main(String... args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table users = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            System.out.print("Updating data...");

            byte[] rowBytesId = Bytes.toBytes("3");

            Get rowId = new Get(rowBytesId);
            Result result = users.get(rowId);

            Cell cell = result.getColumnLatestCell(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_SALARY));
            byte[] rowByte = cell.getValueArray();
            String currentSalary = Bytes.toString(rowByte, cell.getValueOffset(), cell.getValueLength()).replace(",", "");
            Put promotionPut = new Put(rowBytesId);
            promotionPut.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_DESIGNATION), Bytes.toBytes("Sr. Engineer"));
            promotionPut.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_SALARY), Bytes.toBytes(String.format("%,d", (int) (Integer.parseInt(currentSalary) * 1.05))));
            users.put(promotionPut);
            users.close();
            System.out.println("Done!");
        }
    }

    private static final String COL_NAME = "Name";
    private static final String COL_CITY = "City";
    private static final String COL_DESIGNATION = "Designation";
    private static final String COL_SALARY = "Salary";
}

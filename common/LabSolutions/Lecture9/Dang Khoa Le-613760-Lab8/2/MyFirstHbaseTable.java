package com.khoale;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable
{

	private static final String TABLE_NAME = "user";
	private static final String CF_PERSONAL = "personal_details";
	private static final String CF_PROFESSIONAL = "prof_details";
	private static final String COL_NAME = "Name";
	private static final String COL_CITY = "City";
	private static final String COL_DESIGNATION = "Designation";
	private static final String COL_SALARY = "Salary";
	public static void main(String... args) throws IOException
	{
		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
										.setColumnFamilies(Lists.newArrayList(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL),ColumnFamilyDescriptorBuilder.of(CF_PROFESSIONAL))).build();

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println(" Done!");

			System.out.print("Inserting data...");

			Table hTable = connection.getTable(TableName.valueOf(TABLE_NAME));
			List<Put> puts = new ArrayList();
			puts.add(createPut("1", "John", "Boston", "Manager", 150000));
			puts.add(createPut("2", "Mary", "New York", "Sr. Engineer", 130000));
			puts.add(createPut("3", "Bob", "Fremont", "Jr. Engineer", 90000));

			hTable.put(puts);
			hTable.close();

			System.out.println(" Done!");

			System.out.print("Updating data...");

			Get get = new Get(Bytes.toBytes("3"));
			Result result = hTable.get(get);
			Cell cell = result.getColumnLatestCell(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_SALARY));
			byte[] rowByte = cell.getValueArray(); 
			String currentSalary = Bytes.toString(rowByte, cell.getValueOffset(), cell.getValueLength()).replace(",", "");
			Put promotionPut = new Put(Bytes.toBytes("3"));
			promotionPut.addColumn(Bytes.toBytes(CF_PROFESSIONAL),Bytes.toBytes(COL_DESIGNATION) , Bytes.toBytes("Sr. Engineer"));
			promotionPut.addColumn(Bytes.toBytes(CF_PROFESSIONAL),Bytes.toBytes(COL_SALARY) , Bytes.toBytes(String.format("%,d", (int) (Integer.parseInt(currentSalary) * 1.05))));

			hTable.put(promotionPut);
			hTable.close();

			System.out.println(" Done!");
		}
	}

	private static Put createPut(String empId, String name, String city, String designation, Integer salary){
		Put p = new Put(Bytes.toBytes(empId));
		p.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_NAME), Bytes.toBytes(name));
		p.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes(COL_CITY), Bytes.toBytes(city));
		p.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_DESIGNATION), Bytes.toBytes(designation));
		p.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes(COL_SALARY), Bytes.toBytes(String.format("%,d", salary)));
		return p;
	}
}
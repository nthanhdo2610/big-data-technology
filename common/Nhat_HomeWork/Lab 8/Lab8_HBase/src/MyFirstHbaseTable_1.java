import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable
{

	private static final String TABLE_NAME = "user";
	private static final String CF_DEFAULT1 = "personal_details";
	private static final String CF_DEFAULT2 = "prof_details";

	private static final byte[] cfPersonal = Bytes.toBytes(CF_DEFAULT1); //Column family name 1
	private static final byte[] cName = Bytes.toBytes("Name"); //Column family name 1 : Column name 1
	private static final byte[] cCity = Bytes.toBytes("City"); //Column family name 1 : Column name 2
	private static final byte[] cfProf = Bytes.toBytes(CF_DEFAULT2); //Column family name 2
	private static final byte[] cDesignation = Bytes.toBytes("Name"); //Column family name 2 : Column name 1
	private static final byte[] cSalary = Bytes.toBytes("City"); //Column family name 2 : Column name 2

	public static void main(String... args) throws IOException
	{
		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT1).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT2).setCompressionType(Algorithm.NONE));

			System.out.print("Creating table...");

			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println("Done creating table!");
			
			//Question 1: Modify “MyFirstHbaseTable.java” program to create the following table structure and data in HBase
			Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
			try 
			{
				//Obj 1
				Put put1 = new Put(Bytes.toBytes("1"));
				put1.addColumn(cfPersonal, cName, Bytes.toBytes("John"));
				put1.addColumn(cfPersonal, cCity, Bytes.toBytes("Boston"));
				put1.addColumn(cfProf, cDesignation, Bytes.toBytes("Manager"));
				put1.addColumn(cfProf, cSalary, Bytes.toBytes(150000));
				tbl.put(put1);
				
				//Obj 2
				Put put2 = new Put(Bytes.toBytes("2"));
				put2.addColumn(cfPersonal, cName, Bytes.toBytes("Mary"));
				put2.addColumn(cfPersonal, cCity, Bytes.toBytes("New York"));
				put2.addColumn(cfProf, cDesignation, Bytes.toBytes("Sr. Engineer"));
				put2.addColumn(cfProf, cSalary, Bytes.toBytes(130000));
				tbl.put(put2);
				
				//Obj 3
				Put put3 = new Put(Bytes.toBytes("3"));
				put3.addColumn(cfPersonal, cName, Bytes.toBytes("Bob"));
				put3.addColumn(cfPersonal, cCity, Bytes.toBytes("Fremont"));
				put3.addColumn(cfProf, cDesignation, Bytes.toBytes("Jr. Engineer"));
				put3.addColumn(cfProf, cSalary, Bytes.toBytes(90000));			
				tbl.put(put3);
			}
			finally 
			{
				System.out.println("Done adding rows!");
			}
			
			//Question 2: Now programmatically promote Bob to Sr. Engineer position and increase his salary by 5%.
			try 
			{
				byte[] rowkey = Bytes.toBytes("3"); //Bob is the 3rd employee
				
				Result row = tbl.get(new Get(rowkey)); //Get data row from HBase table
				byte[] name = row.getValue(cfPersonal, cName); //Get value of cell Name
				byte[] city = row.getValue(cfPersonal, cCity); //Get value of cell City
				byte[] designation = Bytes.toBytes("Sr. Engineer"); //Promote Bob to Sr. Engineer
				double salary = Bytes.toInt(row.getValue(cfProf, cSalary)) * 1.05; //Increase his salary by 5%
				
				Put updated = new Put(rowkey);
				updated.addColumn(cfPersonal, cName, name);
				updated.addColumn(cfPersonal, cCity, city);
				updated.addColumn(cfProf, cDesignation, designation); 
				updated.addColumn(cfProf, cSalary, Bytes.toBytes(salary)); 	
				tbl.put(updated);
			}
			finally 
			{
				System.out.println("Done promoting emp3!");
			}

			//Question 3: how to find the number of rows in an HBase table
			int count = 0;
			Scan scan = new Scan();
			ResultScanner scanner = tbl.getScanner(scan);
			for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
				count++;
			}
			System.out.println("The number of rows of table user: " + count);

			tbl.close();
			connection.close();
		}
	}
}
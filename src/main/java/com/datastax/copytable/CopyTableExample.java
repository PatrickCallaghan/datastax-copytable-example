package com.datastax.copytable;

import java.util.Iterator;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CopyTableExample {
	
	private static final int TOTAL_RECORDS = 1000000;
	
	private Session session;
	private static String testTableName = "test_table";
	private static String keyspaceName = "test_keyspace_copy";
	private static String copyTable= "copy_table";
	private static String tableName = keyspaceName + "." + testTableName;	
	private static String copyTableName = keyspaceName + "." + copyTable;
	
	public CopyTableExample(){
			
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();		
		this.session = cluster.connect();
		
		System.out.println("Cluster and Session created.");
		
		this.setUp();
		this.createTable();
		this.copyTable();
		this.tearDown();
		
		System.out.println("Batch test finished.");
		
		cluster.shutdown();
	}

	private void createTable() {
		//Set up ColumnFamily
		String createTable = "CREATE TABLE " + tableName + "(user_id text PRIMARY KEY, first text, last text, city text, email text)";

		this.session.execute(createTable);
		System.out.println("Table " + tableName + " created");		

		PreparedStatement stmt = session.prepare("INSERT INTO " + tableName + "(user_id, first, last, city, email) VALUES (?, ?, ?, ?, ?)");
		BoundStatement boundStmt = new BoundStatement(stmt);
		
		//populate 
		for (int i = 0; i < TOTAL_RECORDS; i++){
			session.execute(boundStmt.bind("U" + i, "first" + i, "last" + i, "city" + i ,"email@gmail.com" + i));
		}
		
		System.out.println("Table Populated with " + TOTAL_RECORDS + " records");
	}

	
	private void copyTable() {
				
		String createTable = "CREATE TABLE " + copyTableName + "(user_id text PRIMARY KEY, first text, last text, city text, email text)";

		this.session.execute(createTable);
		System.out.println("Table " + copyTableName + " created");		

		PreparedStatement stmt = session.prepare("INSERT INTO " + copyTableName + "(user_id, first, last, city, email) VALUES (?, ?, ?, ?, ?)");
		BoundStatement boundStmt = new BoundStatement(stmt);
		
		int counter = 0;
		long start = System.currentTimeMillis();
		
		//Page the results from the original table and batch the results back in. 
		Iterator<Row> iterator = session.execute(QueryBuilder.select(new String[]{"user_id", "first", "last", "city", "email"})
				.from(keyspaceName, testTableName)).iterator();
		
		while(iterator.hasNext()){
			
			Row row = iterator.next();
			
			session.execute(boundStmt.bind(row.getString("user_id"), row.getString("first"), 
					row.getString("last"), row.getString("city") ,row.getString("email")));
			
			counter ++;
		}

		long end = System.currentTimeMillis();
		System.out.println("Copied " + counter + " records in " + (end-start)/1000 + "secs");
	}

	public void setUp(){
		
		//Set up Keyspace
		String createKeyspace = "CREATE KEYSPACE " + keyspaceName + " WITH replication = { "
				+ "'class': 'SimpleStrategy', 'replication_factor': '1' }";

		this.session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);		
		this.session.execute(createKeyspace);
		System.out.println("Keyspace " + keyspaceName + " created");		
	}
	
	
	public void tearDown(){
		
		String dropKeyspace = "DROP KEYSPACE " + this.keyspaceName;
		
		this.session.execute(dropKeyspace);
		System.out.println("Keyspace DROPPED");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new CopyTableExample();
	}

}

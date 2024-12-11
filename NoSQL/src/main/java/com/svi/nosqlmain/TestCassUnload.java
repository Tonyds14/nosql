package com.svi.nosqlmain;

import java.net.InetSocketAddress;
import java.time.LocalDate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class TestCassUnload {

	public static void main(String[] args) {
        String contactPoint = "127.0.0.1";  //local machine
//		String contactPoint = "18.209.183.205"; 
		int port = 9042; 
		
		CqlSession session = CqlSession.builder()
			    .addContactPoint(new InetSocketAddress(contactPoint, port))
			    .withLocalDatacenter("datacenter1")
			    .build();
		
		String keyspace = "delossantos_training";
		String table = "crud_training";
		String selectQuery = "SELECT * FROM " + keyspace + "." + table;

		ResultSet resultSet = session.execute(selectQuery);
		for (Row row : resultSet) {
	
		    String id = row.getString("id");
		    String name = row.getString("name"); 
		    int age = row.getInt("age"); 
		    LocalDate birthdate = row.getLocalDate("birthdate"); 
		    
		    System.out.println("id: " + id + ", name: " + name+ ", age: " + age+ ", birthdate: " + birthdate);
		}
		
		session.close();

	}

}

package com.svi.nosqltest;

import java.net.InetSocketAddress;

import java.time.LocalDate;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
//import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
//import com.datastax.oss.driver.api.mapper.annotations.Mapper;

public abstract class CassandraConnector {

    public static void main(String[] args) {
    	dummyConnect();
    }
    
    public static void dummyConnect() {
//        String contactPoint = "18.209.183.205"; // Change this to your Cassandra server's IP
//        String keyspace = "delossantos";
        
        String contactPoint = "127.0.0.1"; // Change this to your Cassandra server's IP
        String keyspace = "delossantos_training";        
        
        String localDatacenter = "datacenter1";
        
        CqlSession session = new CqlSessionBuilder()
        		.addContactPoint(new InetSocketAddress(contactPoint, 9042))
        		.withLocalDatacenter(localDatacenter)
                .withKeyspace(keyspace)
                .build();

        String query = "SELECT id, age, birthdate, name FROM delossantos_training.crud_training";

        ResultSet resultSet = session.execute(query);
        for (Row row : resultSet) {
            String id = row.getString("id");
            int age = row.getInt("age");
            LocalDate birthdate = row.getLocalDate("birthdate");
            String name = row.getString("name");

            System.out.println("ID: " + id + ", Age: " + age + ", Birthdate: " + birthdate + ", Name: " + name);
        }

        session.close();
        
    }


}

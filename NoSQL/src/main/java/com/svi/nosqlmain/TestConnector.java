package com.svi.nosqlmain;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;

public class TestConnector {

	public static void main(String[] args) {
        String contactPoints = "127.0.0.1";  //local machine
//        String contactPoints = "18.209.183.205"; 
        
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoints, 9042))
                .withLocalDatacenter("datacenter1") 
                .build()) {
        	System.out.println("Connected to Cassandra!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

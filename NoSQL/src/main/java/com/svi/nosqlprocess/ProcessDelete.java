package com.svi.nosqlprocess;

import java.net.InetSocketAddress;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class ProcessDelete {
	private static ArrayList<String> notExistingRecords = new ArrayList<>();
	
	public void processDelete(ArrayList<String> deleteRecords, String cassIP,String cassKeySpace,String cassTable) {
		String contactPoints = cassIP;
		
		try (CqlSession session = CqlSession.builder()
				.addContactPoint(new InetSocketAddress(contactPoints, 9042))
				.withLocalDatacenter("datacenter1")
				.build()) {
			String deleteQuery = "DELETE FROM " + cassKeySpace + "." + cassTable + " WHERE id = ?";
            PreparedStatement preparedStatement = session.prepare(deleteQuery);

            int successfulDelete = 0;
            int recNotExisting = 0;
            int exceptionDelete = 0;
            int unsuccessfulDelete = 0;

            for (String data : deleteRecords) {
                try {
                    // Check if the ID already exists
                    String selectQuery = "SELECT id FROM " + cassKeySpace + "." + cassTable + " WHERE id = ?";
                    PreparedStatement selectStatement = session.prepare(selectQuery);
                    BoundStatement selectBoundStatement = selectStatement.bind(data);

                    if (session.execute(selectBoundStatement).one() != null) {
                    	BoundStatement boundStatement = preparedStatement.bind(data);
                        session.execute(boundStatement);
                        successfulDelete++;
                    } else {
                    	notExistingRecords.add(data);
                    	recNotExisting++;
                    	unsuccessfulDelete++;
                    }
                } catch (Exception e) {
                    System.out.println("Failed to delete record: " + data + ", Reason: " + e.getMessage());
                    exceptionDelete++;
                    unsuccessfulDelete++;
                }
            }

            System.out.println("Rows DELETED: " + successfulDelete);
            System.out.println("Unsuccessful DELETE: "+unsuccessfulDelete+"; ID Not Existing("+recNotExisting+") and "+"Exception("+exceptionDelete+")");
            
            if(recNotExisting>0) {
            	printNotExistRec();
            }
            
        }
	}
	
	public static void printNotExistRec() {
		System.out.println("Record does not exist:");
		int ctr = 1;
        for (String ne : notExistingRecords) {
            System.out.println("Rec# "+ctr+" | id: "+ne);
            ctr++;
        }
	}
	
}

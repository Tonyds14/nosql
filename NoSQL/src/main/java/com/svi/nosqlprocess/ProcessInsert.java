package com.svi.nosqlprocess;

import java.net.InetSocketAddress;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.svi.nosqlobj.RecordInfoInsert;

public class ProcessInsert {
	private static ArrayList<RecordInfoInsert> duplicateRecords = new ArrayList<>();
	
	public void processInsert(ArrayList<RecordInfoInsert> inputRecords, String cassIP,String cassKeySpace,String cassTable) {
		String contactPoints = cassIP;
		
		try (CqlSession session = CqlSession.builder()
				.addContactPoint(new InetSocketAddress(contactPoints, 9042))
				.withLocalDatacenter("datacenter1")
				.build()) {
            String insertQuery = "INSERT INTO " + cassKeySpace + "." + cassTable + " (id, name, age, birthdate) VALUES (?, ?, ?, ?)";
            PreparedStatement preparedStatement = session.prepare(insertQuery);

            int successfulInserts = 0;
            int duplicateRecs = 0;
            int exceptionInserts = 0;
            int unsuccessfulInserts = 0;

            for (RecordInfoInsert data : inputRecords) {
                try {
                    // Check if the ID already exists
                    String selectQuery = "SELECT id FROM " + cassKeySpace + "." + cassTable + " WHERE id = ?";
                    PreparedStatement selectStatement = session.prepare(selectQuery);
                    BoundStatement selectBoundStatement = selectStatement.bind(data.getId());

                    if (session.execute(selectBoundStatement).one() != null) {
                        duplicateRecords.add(data);
                        duplicateRecs++;
                        unsuccessfulInserts++;
                    } else {
                        BoundStatement boundStatement = preparedStatement.bind(data.getId(), data.getName(), data.getAge(), data.getBirthdate());
                        session.execute(boundStatement);
                        successfulInserts++;
                    }
                } catch (Exception e) {
                    System.out.println("Failed to insert record: " + data + ", Reason: " + e.getMessage());
                    exceptionInserts++;
                    unsuccessfulInserts++;
                }
            }

            System.out.println("Rows INSERTED: " + successfulInserts);
            System.out.println("Unsuccessful INSERT: "+unsuccessfulInserts+"; Duplicate("+duplicateRecs+") and "+"Exception("+exceptionInserts+")");
            
            if (duplicateRecs>0) {
            	printDupRec();
            }
            
            
        }
        
	}
	
	public static void printDupRec() {
		System.out.println("Records already existing:");
        for (RecordInfoInsert dup : duplicateRecords) {
        	
        	String formattedRecnum = String.format("%-2s", dup.getRecnum());
        	String formattedName = String.format("%-20s", dup.getName());
        	DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
            String formattedBirthdate = dup.getBirthdate().format(inputFormatter).replace("-", "/");
            String swappedBirthdate = formattedBirthdate.substring(5, 10) +"/" + formattedBirthdate.substring(0, 4);
            
            System.out.println("Rec: "+formattedRecnum+"| "+dup.getId()+"  "+formattedName+"  "+dup.getAge()+"  "+swappedBirthdate);
        }
	}


}

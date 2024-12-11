package com.svi.nosqlprocess;

import java.net.InetSocketAddress;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.svi.nosqlobj.RecordInfoUpdate;

public class ProcessUpdate {
	private static ArrayList<RecordInfoUpdate> notExistingRecords = new ArrayList<>();
	private static final int DEFAULT_PORT = 9042;
	private static CqlSession session;
	private static int successfulUpdate = 0;
	private static int recNotExisting = 0;
	private static int exceptionUpdate = 0;
	private static int unsuccessfulUpdate = 0;
	
	public void processUpdate(ArrayList<RecordInfoUpdate> updateRecords, String cassIP,String cassKeySpace,String cassTable) {
		
		session = createSession(cassIP);
		
		for (RecordInfoUpdate data : updateRecords) {
			String function = data.getFunction();
			String id = data.getId();
			
			boolean recordExists = doesRecordExist(cassKeySpace, cassTable, id);
			if (recordExists) {
				evaluateAndPerformFunction(data, function,cassKeySpace,cassTable);
			} else {
				addNotExistingRecord(data);
			}

		}
		
		printSummary();		
		
		session.close();		
            
	}	
	
	public static void printSummary() {
        System.out.println("Rows UPDATED: " + successfulUpdate);
        System.out.println("Unsuccessful UPDATE: "+unsuccessfulUpdate+"; Not Existing("+recNotExisting+") and "+"Exception("+exceptionUpdate+")");

        if(!notExistingRecords.isEmpty()) {
        	printNotExistRec();
        }
        
	}

		
	private CqlSession createSession(String contactPoint) {
        return CqlSession.builder()
            .addContactPoint(new InetSocketAddress(contactPoint, DEFAULT_PORT))
            .withLocalDatacenter("datacenter1")
            .build();
    }
	
	public static boolean doesRecordExist(String cassKeySpace, String cassTable, String id) {
        String selectQuery = "SELECT id FROM " + cassKeySpace + "." + cassTable + " WHERE id = ?";
        PreparedStatement selectStatement = session.prepare(selectQuery);
        BoundStatement selectBoundStatement = selectStatement.bind(id);

        ResultSet resultSet = session.execute(selectBoundStatement);
        Row row = resultSet.one();

        return row != null;
    }
	
	public static void addNotExistingRecord(RecordInfoUpdate data) {
    	notExistingRecords.add(data);
    	recNotExisting++;
    	unsuccessfulUpdate++;
	}
	
	public static void evaluateAndPerformFunction(RecordInfoUpdate data, String function, String cassKeySpace,String cassTable) {
		switch (function) {
        case "updName":
            updateName(cassKeySpace,cassTable, data);
            break;
        case "updAge":
        	updateAge(cassKeySpace,cassTable, data);
            break;
        case "updBdate":
        	updateBirthDate(cassKeySpace,cassTable, data);
            break;
        case "updNameAge":
        	updateNameAge(cassKeySpace,cassTable, data);
            break;
        case "updNameBdate":
        	updateNameBirthDate(cassKeySpace,cassTable, data);
            break;
        case "updAgeBdate":
        	updateAgeBirthDate(cassKeySpace,cassTable, data);
            break;
        case "updNameAgeBdate":
        	updateNameAgeBirthDate(cassKeySpace,cassTable, data);
            break;
        default:
            System.out.println("Unknown function");
            break;
		}
	}
	
	public static void updateName(String cassKeySpace,String cassTable, RecordInfoUpdate data) {
		try {
			String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET name = ? WHERE id = ?";
	        PreparedStatement preparedStatement = session.prepare(updateQuery);

	        BoundStatement boundStatement = preparedStatement.bind(data.getName(), data.getId());
            session.execute(boundStatement);
            successfulUpdate++;
		} catch (Exception e) {
            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
            exceptionUpdate++;
            unsuccessfulUpdate++;
        }
	}
	
	public static void updateAge(String cassKeySpace,String cassTable, RecordInfoUpdate data) {
		try {
			String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET age = ? WHERE id = ?";
	        PreparedStatement preparedStatement = session.prepare(updateQuery);

	        BoundStatement boundStatement = preparedStatement.bind(data.getAge(), data.getId());
            session.execute(boundStatement);
            successfulUpdate++;
		} catch (Exception e) {
            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
            exceptionUpdate++;
            unsuccessfulUpdate++;
        }
	}
	
	public static void updateBirthDate(String cassKeySpace,String cassTable, RecordInfoUpdate data) {
		try {
			String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET birthdate = ? WHERE id = ?";
	        PreparedStatement preparedStatement = session.prepare(updateQuery);

	        BoundStatement boundStatement = preparedStatement.bind(data.getBirthdate(), data.getId());
            session.execute(boundStatement);
            successfulUpdate++;
		} catch (Exception e) {
            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
            exceptionUpdate++;
            unsuccessfulUpdate++;
        }
	}
	
	public static void updateNameAge(String cassKeySpace,String cassTable, RecordInfoUpdate data) {
		try {
			String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET name = ?, age = ? WHERE id = ?";
	        PreparedStatement preparedStatement = session.prepare(updateQuery);

	        BoundStatement boundStatement = preparedStatement.bind(data.getName(), data.getAge(), data.getId());
            session.execute(boundStatement);
            successfulUpdate++;
		} catch (Exception e) {
            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
            exceptionUpdate++;
            unsuccessfulUpdate++;
        }
	}
	
	public static void updateAgeBirthDate(String cassKeySpace,String cassTable, RecordInfoUpdate data) {
		try {
			String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET age = ?, birthdate = ? WHERE id = ?";
	        PreparedStatement preparedStatement = session.prepare(updateQuery);

	        BoundStatement boundStatement = preparedStatement.bind(data.getAge(), data.getBirthdate(), data.getId());
            session.execute(boundStatement);
            successfulUpdate++;
		} catch (Exception e) {
            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
            exceptionUpdate++;
            unsuccessfulUpdate++;
        }
	}
	
	public static void updateNameBirthDate(String cassKeySpace,String cassTable, RecordInfoUpdate data) {
		try {
			String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET name = ?, birthdate = ? WHERE id = ?";
	        PreparedStatement preparedStatement = session.prepare(updateQuery);

	        BoundStatement boundStatement = preparedStatement.bind(data.getName(), data.getBirthdate(), data.getId());
            session.execute(boundStatement);
            successfulUpdate++;
		} catch (Exception e) {
            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
            exceptionUpdate++;
            unsuccessfulUpdate++;
        }
	}
	
	public static void updateNameAgeBirthDate(String cassKeySpace,String cassTable, RecordInfoUpdate data) {
		try {
			String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET name = ?, age = ?, birthdate = ? WHERE id = ?";
	        PreparedStatement preparedStatement = session.prepare(updateQuery);

	        BoundStatement boundStatement = preparedStatement.bind(data.getName(), data.getAge(), data.getBirthdate(), data.getId());
            session.execute(boundStatement);
            successfulUpdate++;
		} catch (Exception e) {
            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
            exceptionUpdate++;
            unsuccessfulUpdate++;
        }
		
	}
	
	public static void printNotExistRec() {
		System.out.println("Records does not exist:");
        for (RecordInfoUpdate ne : notExistingRecords) {
        	
        	String formattedRecnum = String.format("%-2s", ne.getRecnum());
        	String formattedName = String.format("%-20s", ne.getName());
        	DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
//	            String formattedBirthdate = ne.getBirthdate().format(inputFormatter).replace("-", "/");
//	            String swappedBirthdate = formattedBirthdate.substring(5, 10) +"/" + formattedBirthdate.substring(0, 4);
            
            String swappedBirthdate = "";
            if (ne.getFunction().equals("updBdate") ||
            	    ne.getFunction().equals("updNameBdate") ||
            	    ne.getFunction().equals("updAgeBdate") ||
            	    ne.getFunction().equals("updNameAgeBdate")) {
            	    String formattedBirthdate = ne.getBirthdate().format(inputFormatter).replace("-", "/");
            	    swappedBirthdate = formattedBirthdate.substring(5, 10) + "/" + formattedBirthdate.substring(0, 4);
            }            

			System.out.println("Rec: "+formattedRecnum+"| "+ne.getId()+"  "+formattedName+"  "+ne.getAge()+"  "+swappedBirthdate);
        }
	}

}


//String contactPoints = cassIP;
//
//try (CqlSession session = CqlSession.builder()
//		.addContactPoint(new InetSocketAddress(contactPoints, 9042))
//		.withLocalDatacenter("datacenter1")
//		.build()) {
//
//	String updateQuery = "UPDATE " + cassKeySpace + "." + cassTable + " SET name = ?, age = ?, birthdate = ? WHERE id = ?";
//    PreparedStatement preparedStatement = session.prepare(updateQuery);
//
//    int successfulUpdate = 0;
//    int recNotExisting = 0;
//    int exceptionUpdate = 0;
//    int unsuccessfulUpdate = 0;
//
//    for (RecordInfoInsert data : inputRecords) {
//        try {
//            // Check if the ID already exists
//            String selectQuery = "SELECT id FROM " + cassKeySpace + "." + cassTable + " WHERE id = ?";
//            PreparedStatement selectStatement = session.prepare(selectQuery);
//            BoundStatement selectBoundStatement = selectStatement.bind(data.getId());
//
//            if (session.execute(selectBoundStatement).one() != null) {
//            	BoundStatement boundStatement = preparedStatement.bind(data.getName(), data.getAge(), data.getBirthdate(),data.getId());
//                session.execute(boundStatement);
//                successfulUpdate++;
//            } else {
//            	notExistingRecords.add(data);
//            	recNotExisting++;
//            	unsuccessfulUpdate++;
//            }
//        } catch (Exception e) {
//            System.out.println("Failed to update record: " + data + ", Reason: " + e.getMessage());
//            exceptionUpdate++;
//            unsuccessfulUpdate++;
//        }
//    }
//
//    System.out.println("Rows UPDATED: " + successfulUpdate);
//    System.out.println("Unsuccessful UPDATE: "+unsuccessfulUpdate+"; ID Not Existing("+recNotExisting+") and "+"Exception("+exceptionUpdate+")");
//    
//    if(recNotExisting>0) {
//    	printNotExistRec();
//    }
//}

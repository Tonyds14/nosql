package com.svi.nosqlprocess;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.svi.nosqlobj.RecordInfoInsert;
	
public class GetDataInsert {

	private static int MAX_ROWS = 55;
	private static ArrayList<String> rejectedRecords = new ArrayList<>();
	private static ArrayList<RecordInfoInsert> inputRecords = new ArrayList<>();

	private static int countReject = 0;
	private static int countForProcess = 0;
	private static int countInputRecords = 0;
	
	public static ArrayList<RecordInfoInsert> getInputData(File file) {
		int startRow = 1;
		
        FileInputStream fis = null;        

        try {
            fis = new FileInputStream(file);
            Workbook workbook = WorkbookFactory.create(fis);

            for (Sheet sheet : workbook) {  // Iterate over each sheet in the workbook
                for (int rowNum = startRow; rowNum <= MAX_ROWS; rowNum++) {  
                	Row row = sheet.getRow(rowNum);
                	if (row != null) {
                			
                		countInputRecords++;
                		
	        			 Cell idCell = row.getCell(0, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
	                     Cell nameCell = row.getCell(1, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
	                     Cell ageCell = row.getCell(2, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
	                     Cell birthdateCell = row.getCell(3, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
	                     
	                     String id = getCellValueAsString(idCell);
	                     String name = getCellValueAsString(nameCell);
	                     String age = getCellValueAsString(ageCell);
	                     String birthdateString = getCellValueAsString(birthdateCell);
                         
//                         System.out.println("Row: " + rowNum + ", ID: " + id + ", Name: " + name + ", Age: " + age +", Birthdate: " + birthdateString );
            			 
                         int recNum = rowNum;
                         
                         validateAndBuildInputData(recNum, id,name,age,birthdateString);
                         
            		}
            	} //for row loop
        	} //for sheet loop
            
            printSummary();            
            
            
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return inputRecords;
    }
	

	public static String getCellValueAsString(Cell cell) {
        String cellValue = "";
        if (cell != null) {
            switch (cell.getCellType()) {
                case STRING:
                    cellValue = cell.getStringCellValue();
                    break;
                case NUMERIC:
                	if (DateUtil.isCellDateFormatted(cell)) {
                        Date dateValue = cell.getDateCellValue();
                        cellValue = formatDateValue(dateValue);
                    } else {
                    	cellValue = formatNumericValue(cell.getNumericCellValue());
                    }
                    break;
                case BOOLEAN:
                    cellValue = String.valueOf(cell.getBooleanCellValue());
                    break;
                case FORMULA:
                    cellValue = cell.getCellFormula();
                    break;
                case BLANK:
                    cellValue = "";
                    break;
			default:
				break;
            }
        }
        return cellValue;
    }
	
	public static String formatNumericValue(double numericValue) {
        DecimalFormat decimalFormat = new DecimalFormat("#");
        return decimalFormat.format(numericValue);
    }
	
	public static String formatDateValue(Date dateValue) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(dateValue);
    }
	
	
	public static void validateAndBuildInputData(int recNum, String id, String name, String age, String birthdateString) {
		
		String rejectReason = null;
		
		if (id.isEmpty()) {
    	    rejectReason = "ID is blank";
    	} else if (name.isEmpty()) {
    	    rejectReason = "Name is blank";
    	}
         
        if (rejectReason != null) {
        	String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", " + age + ", " + birthdateString + ", Reason: " + rejectReason;
        	rejectedRecords.add(rejectedRecord);
        	countReject++;
        } else {
        		
        	int intAge = 0;
                
        	try {
        		intAge = Integer.parseInt(age);
               	 
               	if (intAge < 1) {

               		rejectReason = "Age less than 1";
                	String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", "+ age +", " + birthdateString +", Reason: "+rejectReason;
                	rejectedRecords.add(rejectedRecord);
                	countReject++;
                        
               	} else {
                          
               		try {
               			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                       	LocalDate birthdate = LocalDate.parse(birthdateString, formatter);

                       	RecordInfoInsert record = new RecordInfoInsert(recNum, id, name, intAge, birthdate);
                       	inputRecords.add(record);
                       	countForProcess++;
                                 
               		} catch (DateTimeParseException e) {
	  		
               			rejectReason = "Error parsing birthdate";
               			String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", "+ age +", " + birthdateString +", Reason: "+rejectReason;
               			rejectedRecords.add(rejectedRecord);
               			countReject++;
               		} 
                         
               	}    
        	} catch (NumberFormatException e) {

        		rejectReason = "Error parsing age";
        		String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", "+ age + ", " + birthdateString +", Reason: "+rejectReason;
        		rejectedRecords.add(rejectedRecord);
        		countReject++;
        	}
        }
	
	}
	
	public static void printSummary() {
		System.out.println("Total Input Records: "+countInputRecords+"; #Recs for Processing: "+countForProcess+" #Recs Rejected: "+ countReject);
        if(!rejectedRecords.isEmpty()) {
        	System.out.println("Rejected Records: ");
        	for(String rejects: rejectedRecords) {
            	System.out.println(rejects);
            }
        }
	}

}

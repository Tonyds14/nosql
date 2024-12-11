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

import com.svi.nosqlobj.RecordInfoUpdate;
	
public class GetDataUpdate {

	private static int MAX_ROWS = 55;
	private static ArrayList<String> rejectedRecords = new ArrayList<>();
	private static ArrayList<RecordInfoUpdate> updateRecords = new ArrayList<>();

	private static int countReject = 0;
	private static int countForProcess = 0;
	private static int countInputRecords = 0;
	private static String rejectReason = null;	
	private static int intAge = 0;
	private static String function ="";
	private static boolean validAge =false;
	private static boolean validBirthDate =false;
	
	private static LocalDate birthdate = LocalDate.parse("1900-01-01");
	
	public static ArrayList<RecordInfoUpdate> getInputData(File file) {
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
	                     
                         validateAndBuildInputData(recNum, id,name,age,birthdateString, function);
                         
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

        return updateRecords;
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
	
	
	public static void validateAndBuildInputData(int recNum, String id, String name, String age, String birthdateString, String function) {
		
		rejectReason = null;		
        	
		if (id.isEmpty()) {
    	    rejectReason = "ID is blank";
    	} 
         
        if (rejectReason != null) {
        	String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", " + age + ", " + birthdateString + ", Reason: " + rejectReason;
        	rejectedRecords.add(rejectedRecord);
        	countReject++;
        } else {	
        	
            defineFunction(recNum, id, name, age, birthdateString);

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
	
	public static void defineFunction(int recNum, String id, String name, String age, String birthdateString) {
		function = "";
		validAge = false;
		validBirthDate = false;
		birthdate = LocalDate.parse("1900-01-01");
		
		if (!name.isEmpty() && age.isEmpty() && birthdateString.isEmpty()) {
            function = "updName";
            
            AddUpdateRecords(recNum, id, name, age, birthdateString);
            
        } else if (name.isEmpty() && !age.isEmpty() && birthdateString.isEmpty()) {
            function = "updAge";
            validAge = validateAge(recNum, id, name, age, birthdateString);
            
            if(validAge) {
            	AddUpdateRecords(recNum, id, name, age, birthdateString);
            }
            
        } else if (name.isEmpty() && age.isEmpty() && !birthdateString.isEmpty()) {
            function = "updBdate";
            validBirthDate = validateBirthDate(recNum, id, name, age, birthdateString);
            
            if(validBirthDate) {
            	AddUpdateRecords(recNum, id, name, age, birthdateString);
            }
            
        } else if (!name.isEmpty() && !age.isEmpty() && birthdateString.isEmpty()) {
            function = "updNameAge";
            validAge = validateAge(recNum, id, name, age, birthdateString);
            
            if(validAge) {
            	AddUpdateRecords(recNum, id, name, age, birthdateString);
            }
            
        } else if (!name.isEmpty() && age.isEmpty() && !birthdateString.isEmpty()) {
            function = "updNameBdate";
            validBirthDate = validateBirthDate(recNum, id, name, age, birthdateString);
            
            if(validBirthDate) {
            	AddUpdateRecords(recNum, id, name, age, birthdateString);
            }
            
        } else if (name.isEmpty() && !age.isEmpty() && !birthdateString.isEmpty()) {
            function = "updAgeBdate"; 
            validAge = validateAge(recNum, id, name, age, birthdateString);
            
            if(validAge) {
            	validBirthDate = validateBirthDate(recNum, id, name, age, birthdateString);
            }            
            
            if(validBirthDate) {
            	AddUpdateRecords(recNum, id, name, age, birthdateString);
            }
            
        } else if (!name.isEmpty() && !age.isEmpty() && !birthdateString.isEmpty()) {
            function = "updNameAgeBdate";
            validAge = validateAge(recNum, id, name, age, birthdateString);
            validBirthDate = validateBirthDate(recNum, id, name, age, birthdateString);
            
            if(validAge) {
            	validBirthDate = validateBirthDate(recNum, id, name, age, birthdateString);
            }            
            
            if(validBirthDate) {
            	AddUpdateRecords(recNum, id, name, age, birthdateString);
            }
        } else if (name.isEmpty() && age.isEmpty() && birthdateString.isEmpty()) {
        	rejectReason = "All input field is blank. no reference data for update";
        	String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", " + age + ", " + birthdateString + ", Reason: " + rejectReason;
        	rejectedRecords.add(rejectedRecord);
        	countReject++;
        }


	}
	
	
	public static boolean validateAge(int recNum, String id, String name, String age, String birthdateString) {
		intAge = 0;
		boolean validAge = false;
        
    	try {
    		intAge = Integer.parseInt(age);
           	 
           	if (intAge < 1) {

           		rejectReason = "Age less than 1";
            	String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", "+ age +", " + birthdateString +", Reason: "+rejectReason;
            	rejectedRecords.add(rejectedRecord);
            	countReject++;
                    
           	} else {
           		validAge = true;      
                     
           	}    
    	} catch (NumberFormatException e) {

    		rejectReason = "Error parsing age";
    		String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", "+ age + ", " + birthdateString +", Reason: "+rejectReason;
    		rejectedRecords.add(rejectedRecord);
    		countReject++;
    	}
    	return validAge;
	}
	
	
	public static boolean validateBirthDate(int recNum, String id, String name, String age, String birthdateString) {
		boolean validBirthDate = false;
		
		try {
   			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
           	birthdate = LocalDate.parse(birthdateString, formatter);

           	validBirthDate = true;
                     
   		} catch (DateTimeParseException e) {
	
   			rejectReason = "Error parsing birthdate";
   			String rejectedRecord = "Rec: " + recNum + ", ID: " + id + ", " + name + ", "+ age +", " + birthdateString +", Reason: "+rejectReason;
   			rejectedRecords.add(rejectedRecord);
   			countReject++;
   		} 
		
		return validBirthDate;
	}
	
	public static void AddUpdateRecords(int recNum, String id, String name, String age, String birthdateString) {
		RecordInfoUpdate record = new RecordInfoUpdate(recNum, id, name, intAge, birthdate, function);
       	updateRecords.add(record);
       	countForProcess++;
		
	}
}

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
	
public class GetDataDelete {

	private static int MAX_ROWS = 55;
	
	public static ArrayList<String> getInputData(File file) {
		ArrayList<String> inputRecords = new ArrayList<>();
		
		int countInputRecords = 0;
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
	                     
	                     String id = getCellValueAsString(idCell);

                         inputRecords.add(id);

                         
            		}
            	} //for row loop
        	} //for sheet loop
            
            System.out.println("Total Input Records: "+countInputRecords);
            
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

}

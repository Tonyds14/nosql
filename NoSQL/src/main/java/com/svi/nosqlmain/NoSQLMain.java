package com.svi.nosqlmain;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.TeeOutputStream;

import com.svi.nosqlobj.RecordInfoInsert;
import com.svi.nosqlobj.RecordInfoUpdate;
import com.svi.nosqlprocess.GetConfigInfo;
import com.svi.nosqlprocess.GetDataDelete;
import com.svi.nosqlprocess.GetDataInsert;
import com.svi.nosqlprocess.GetDataUpdate;
import com.svi.nosqlprocess.ProcessDelete;
import com.svi.nosqlprocess.ProcessInsert;
import com.svi.nosqlprocess.ProcessUpdate;

public class NoSQLMain {

	private static String action ="";
    private static String inputFilePath ="";
    private static String cassKeySpace ="";
    private static String cassTable ="";
    private static String cassIP ="";
    private static String outputFilePath ="";
    
    private static List<File> listFiles = new ArrayList<>();
    private static ArrayList<RecordInfoInsert> insertRecords = new ArrayList<>();    
    private static ArrayList<String> deleteRecords = new ArrayList<>();
    private static ArrayList<RecordInfoUpdate> updateRecords = new ArrayList<>(); 
    
    private static boolean isValidConfig= false;
    
	public static void main(String[] args) {
		getConfigData();
		if(isValidConfig & !listFiles.isEmpty() ) {	
			initializeOutputLogFile();
//			getInputData();
			executeAction();
		}	

	}
	
	public static void getConfigData() {
		GetConfigInfo config = new GetConfigInfo();
		action = config.getAction();
		inputFilePath = config.getInputFilePath();
		cassKeySpace = config.getCassKeySpace();
		cassTable = config.getCassTable();
		cassIP = config.getCassIP();
		outputFilePath = config.getOutputFilePath();
		listFiles = config.getListFiles();
		isValidConfig = config.isValidConfig();
	}
	
	public static void initializeOutputLogFile() {
		try {
            FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath + "log.txt");
            TeeOutputStream teeOutputStream = new TeeOutputStream(System.out, fileOutputStream);
            System.setOut(new PrintStream(teeOutputStream, true)); // Auto-flush enabled
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
	}
	
	public static void getInputDataInsert() {
			
		for (final File file : listFiles) {
			insertRecords = GetDataInsert.getInputData(file);
        }	
		
//		System.out.println("inputRecords");
//		for(RecordInfo data: inputRecords) {
//			System.out.println(data.getRecnum()+" "+ data.getId()+" "+ data.getName()+" "+data.getAge()+" "+data.getBirthdate());
//		}

	}
	
	public static void getInputDataDelete() {
		
		for (final File file : listFiles) {
			deleteRecords = GetDataDelete.getInputData(file);
        }	
		
	}
	
	public static void getInputDataUpdate() {
		
		for (final File file : listFiles) {
			updateRecords = GetDataUpdate.getInputData(file);
        }	
		
	}
	
	
	public static void executeAction() {
		action = action.toUpperCase();
		
		switch (action) {
        case "INSERT":
        	getInputDataInsert();
        	
            System.out.println("\nPerforming INSERT operation");

            ProcessInsert insert = new ProcessInsert();
            insert.processInsert(insertRecords,cassIP,cassKeySpace,cassTable);
            
            break;            
        case "DELETE":
        	getInputDataDelete();
        	
            System.out.println("\nPerforming DELETE operation");
            
            ProcessDelete delete = new ProcessDelete();
            delete.processDelete(deleteRecords, cassIP, cassKeySpace, cassTable);
                        
            break;
        case "UPDATE":
        	getInputDataUpdate();
        	
            System.out.println("\nPerforming UPDATE operation");
            
            ProcessUpdate update = new ProcessUpdate();
            update.processUpdate(updateRecords, cassIP, cassKeySpace, cassTable);
            
            break;
        default:
            System.out.println("\nInvalid action");

            break;
		}
		
	}

}

package com.svi.nosqlprocess;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.sonatype.guice.bean.containers.Main;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class GetConfigInfo {
    private static Properties properties;  
    private static String projectPath = "";
    public String inputDirectory ="";
    public String outputDirectory ="";
    
    private static String action ="";
    private static String inputFilePath ="";
    private static String cassKeySpace ="";
    private static String cassTable ="";
    private static String cassIP ="";
    private static String outputFilePath ="";
    
    private static boolean isValidConfig= false;
    private static List<File> listFiles = new ArrayList<>();
    
    public GetConfigInfo() {
        loadProperties();
    }
    
    private void loadProperties() {
        properties = new Properties();
            
        try 
        	(InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("config/config.properties")) {
              properties.load(inputStream);
              
              projectPath = System.getProperty("user.dir");
              
              getConfigData();
              
              isValidConfig = validateConfigData();    
              listFiles = getListOfExcelFiles(inputFilePath);
              
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    public static void getConfigData() {
    	action = properties.getProperty("ACTION");    	
        inputFilePath = properties.getProperty("INPUT_FILEPATH");
        inputFilePath = inputFilePath.replace("${projectPath}", projectPath);

        outputFilePath = properties.getProperty("LOG_FILE_OUTPUT_PATH");
        outputFilePath = outputFilePath.replace("${projectPath}", projectPath);
        
        cassIP = properties.getProperty("CASSANDRA_IP"); 
        cassKeySpace = properties.getProperty("CASSANDRA_KEYSPACE");    	
        cassTable = properties.getProperty("CASSANDRA_TABLE");      	
    	
    	System.out.println(action);
    	System.out.println(inputFilePath);
    	System.out.println(cassKeySpace);
    	System.out.println(cassTable);
    	System.out.println(cassIP);
    	System.out.println(outputFilePath);
    	
    }
    
    public static boolean validateConfigData() {
        if (!isValidAction(action)) {
            System.out.println("Action is not valid.");
            return false;
        }

        if (!doesFilePathExist(inputFilePath)) {
            System.out.println("Input File path does not exist.");
            return false;
        }

        if (!isValidCassandraIP(cassIP)) {
            System.out.println("Cassandra IP is not valid.");
            return false;
        } else {
        	if (!doesKeyspaceExist(cassKeySpace)) {
                System.out.println("Keyspace does not exist.");
//              return false;
                createKeySpace(cassKeySpace);
            }         	
        	
        	if (!doesTableExist(cassKeySpace, cassTable)) {
                System.out.println("Table does not exist.");
//              return false;
                createTable(cassKeySpace, cassTable);
            }
                      	
        }
        
        if (!doesFilePathExist(outputFilePath)) {
            System.out.println("Output File path does not exist.");
            return false;
        }

        System.out.println("All configurations are valid.");
        return true;
    }
    
    public static boolean isValidAction(String action) {
        return action.equalsIgnoreCase("INSERT") ||
               action.equalsIgnoreCase("DELETE") ||
               action.equalsIgnoreCase("UPDATE");
    }
    
    public static boolean doesFilePathExist(String filePath) {
        Path path = Paths.get(filePath);
        return Files.exists(path);
    }
    
    public static boolean doesKeyspaceExist(String keyspaceName) {
    	String contactPoints = cassIP;
    	
        try (CqlSession session = CqlSession.builder()
        		.addContactPoint(new InetSocketAddress(contactPoints, 9042))
				.withLocalDatacenter("datacenter1")
        		.build()) {
            ResultSet resultSet = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspaceName);
            Row row = resultSet.one();
            return row != null;
        }
    }
    
    public static boolean doesTableExist(String keyspaceName, String tableName) {
    	String contactPoints = cassIP;
    	
        try (CqlSession session = CqlSession.builder()
        		.addContactPoint(new InetSocketAddress(contactPoints, 9042))
				.withLocalDatacenter("datacenter1")
        		.build()) {
            ResultSet resultSet = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", keyspaceName, tableName);
            Row row = resultSet.one();
            return row != null;
        }
    }
    
    public static boolean isValidCassandraIP(String ipAddress) {
        try {
            InetAddress address = InetAddress.getByName(ipAddress);
            return address.isReachable(5000); // Adjust the timeout as needed
        } catch (Exception e) {
            return false;
        }
    }
    
    public static List<File> getListOfExcelFiles(String path) {

    	List<File> excelFiles = new ArrayList<>();
    	Path directoryPath = Paths.get(path);
    	
    	File[] files = directoryPath.toFile().listFiles();
        if (files.length > 0) {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".xls") || file.getName().endsWith(".xlsx")) {
                    excelFiles.add(file);
                }
            }
        } else {
        	System.out.println("No excel files found in Input Folder: " +path);
        }        
		return excelFiles;
    }
    
    public static void createKeySpace(String keyspaceName) {
    	String contactPoints = cassIP;
    	
    	CqlSession session = CqlSession.builder()
    			.addContactPoint(new InetSocketAddress(contactPoints, 9042))
				.withLocalDatacenter("datacenter1")
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.SESSION_KEYSPACE, "system")
                        .build())
                .build();
    	
    	int replicationFactor = 3;
    	
    	// Build the CREATE KEYSPACE statement
        String createKeyspaceCql = String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }",
                keyspaceName, replicationFactor
        );
    	
    	session.execute(createKeyspaceCql);
    	
    	System.out.println(keyspaceName +" Keyspace created.");
    	
    	session.close();
    }
    
    public static void createTable(String keyspaceName, String tableName) {
    	String contactPoints = cassIP;
    	
    	CqlSession session = CqlSession.builder()
    			.addContactPoint(new InetSocketAddress(contactPoints, 9042))
				.withLocalDatacenter("datacenter1")
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.SESSION_KEYSPACE, "system")
                        .build())
                .build();
    	// Build the CREATE TABLE statement
    	String createTableCql = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                "id text PRIMARY KEY," +
                "name text," +
                "age int," +
                "birthdate date" +
                ")",
                keyspaceName, tableName
        );
    	
    	session.execute(createTableCql);
    	System.out.println(tableName+" table created.");
    	
    	session.close();
    	
    }


	public String getAction() {
		return action;
	}

	public String getInputFilePath() {
		return inputFilePath;
	}

	public String getCassKeySpace() {
		return cassKeySpace;
	}

	public String getCassTable() {
		return cassTable;
	}

	public String getCassIP() {
		return cassIP;
	}

	public String getOutputFilePath() {
		return outputFilePath;
	}

	public boolean isValidConfig() {
		return isValidConfig;
	}

	public List<File> getListFiles() {
		return listFiles;
	}    
    
	
}

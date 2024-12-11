package com.svi.db.sample;

import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

public class Sampler {
	
//	private static CassandraAccessor accessor;
	private static Cluster cluster;
//	private static MappingManager manager;
//	private static Session session;

	public static void main(String[] args) {

		Session  session = cluster.connect("delossantos");
	    MappingManager  manager = new MappingManager(session);
	    CassandraAccessor  accessor = manager.createAccessor(CassandraAccessor.class);

	    Result<SampleTable> queryAll = getAllRecords();
	    

	}
	
//    public static Result<SampleTable> getSingleRecord(UUID id, int age, String name, LocalDate date) {
//        return accessor.getRecord(id, age, name, date);
//    }
// 
//    public static Result<SampleTable> getAllRecords() {
//        return accessor.getAllRecords();
//    }
    
    public static Result<SampleTable> getAllRecords() {
        return getAllRecords();
    }


}

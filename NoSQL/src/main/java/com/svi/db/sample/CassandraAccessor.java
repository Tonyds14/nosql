package com.svi.db.sample;

import java.util.UUID;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

@Accessor
public interface CassandraAccessor {
	
	@Query("SELECT * FROM sample_table WHERE id = :ID AND age = :AGE AND name = :NAME and birthdate = :DATE ALLOW FILTERING")
	Result<SampleTable> getRecord(@Param("ID") UUID uuid,
			@Param("AGE") int age,@Param("NAME") String dsdsaf, @Param("DATE") LocalDate date);
	
	@Query("SELECT * FROM records")
	Result<SampleTable> getAllRecords();

}

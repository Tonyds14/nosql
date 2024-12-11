package com.svi.db.sample;

import java.util.UUID;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(name = "records", caseSensitiveTable = false)
public class SampleTable {
	
	@PartitionKey(0)
	@Column(name = "id")
	private UUID id;
	
	@ClusteringColumn(0)
	@Column(name = "activity")
	private String activity;
	
	@ClusteringColumn(1)
	@Column(name = "timestamp")
	private String timestamp;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getActivity() {
		return activity;
	}

	public void setAge(String activity) {
		this.activity = activity;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	
	
}

package com.svi.nosqlobj;

import java.time.LocalDate;

public class RecordInfoUpdate {
	private int recnum;
	private String id;
	private String name;
    private int age;
    private LocalDate birthdate;
    private String function;

	public RecordInfoUpdate(int recnum, String id, String name, int age, LocalDate birthdate,String function) {
		this.recnum = recnum;
		this.id = id;
        this.name = name;
        this.age = age;
        this.birthdate = birthdate;
        this.function = function;
    }


	public int getRecnum() {
		return recnum;
	}

	public void setRecnum(int recnum) {
		this.recnum = recnum;
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public LocalDate getBirthdate() {
		return birthdate;
	}

	public void setBirthdate(LocalDate birthdate) {
		this.birthdate = birthdate;
	}
	
	@Override
    public String toString() {
        return "RecordInfo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", birthdate=" + birthdate +
                '}';
    }


	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}
	
}

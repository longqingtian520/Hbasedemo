package com.criss.wang.entity;

public class Employee {
	private String name;

	private String city;

	private String manager;

	private String salary;

	public Employee() {

	}

	public Employee(String name, String city, String manager, String salary) {
		super();
		this.name = name;
		this.city = city;
		this.manager = manager;
		this.salary = salary;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getManager() {
		return manager;
	}

	public void setManager(String manager) {
		this.manager = manager;
	}

	public String getSalary() {
		return salary;
	}

	public void setSalary(String salary) {
		this.salary = salary;
	}
}

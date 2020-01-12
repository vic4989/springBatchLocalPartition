package com.vic.batch.repository;

import org.springframework.data.repository.CrudRepository;

import com.vic.batch.vo.Employee;

public interface EmployeeRepository extends CrudRepository<Employee, String> {

}

package com.vic.batch.controller;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.vic.batch.repository.EmployeeRepository;
import com.vic.batch.vo.Employee;

@RestController
public class EmployeeController {
	
	@Autowired
    JobLauncher jobLauncher;
     
    @Autowired
    Job job;
    
    @Autowired
    private EmployeeRepository repo;

	@RequestMapping("launch")
	public String launchJob() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		JobParameters params = new JobParametersBuilder()
                .addString("JobID", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();
		jobLauncher.run(job, params);
		return "Job Successful";
	}
	
	@RequestMapping("employees")
	public List<Employee> fetchEmployees() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		List<Employee> employees = (List<Employee>) repo.findAll();
		return employees;
	}
	
}

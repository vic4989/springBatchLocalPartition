package com.vic.batch.config;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.vic.batch.partitioner.RangePartitioner;
import com.vic.batch.repository.EmployeeRepository;
import com.vic.batch.vo.Employee;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	@Autowired
    private JobBuilderFactory jobBuilderFactory;
	
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private EmployeeRepository repo;
    
    @Bean
    public Job PartitionJob() {
      return jobBuilderFactory.get("partitionJob").incrementer(new RunIdIncrementer())
          .start(masterStep()).build();
    }

    @Bean
    public Step masterStep() {
      return stepBuilderFactory.get("masterStep").partitioner(slave().getName(), rangePartitioner())
          .partitionHandler(masterSlaveHandler()).build();
    }
   
    @Bean
    public PartitionHandler masterSlaveHandler() {
      TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
      handler.setGridSize(10);
      handler.setTaskExecutor(taskExecutor());
      handler.setStep(slave());
      try {
        handler.afterPropertiesSet();
      } catch (Exception e) {
        e.printStackTrace();
      }
      return handler;
    }
   
    @Bean
    public SimpleAsyncTaskExecutor taskExecutor() {
      return new SimpleAsyncTaskExecutor();
    }

    @Bean(name = "slave")
    public Step slave() {
      System.out.println("...........called slave .........");
   
      return stepBuilderFactory.get("slave").<Employee, Employee>chunk(100)
          .reader(employeeItemReader(null, null, null))
          .processor(employeeItemProcessor(null)).writer(employeeItemWriter(null, null)).build();
    }
   
    @Bean
    public RangePartitioner rangePartitioner() {
      return new RangePartitioner();
    }
    
    @Bean
    @StepScope
    FlatFileItemReader<Employee> employeeItemReader(@Value("#{stepExecutionContext[fromId]}") final String fromId,
    	      @Value("#{stepExecutionContext[toId]}") final String toId,
    	      @Value("#{stepExecutionContext[name]}") final String name) {
        
        FlatFileItemReader<Employee> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("employees.csv"));

        DefaultLineMapper defaultLineMapper = new DefaultLineMapper();
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setNames(new String[] {"firstName", "lastName", "age", "salary"});

        BeanWrapperFieldSetMapper<Employee> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Employee.class);

        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);
        reader.setLineMapper(defaultLineMapper);

        return reader;
    }
    
    @Bean
    public PagingQueryProvider queryProvider() {
      System.out.println("queryProvider start ");
      SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();
      provider.setDataSource(dataSource);
      provider.setSelectClause("select empId, firstName, lastName, salary, age");
      provider.setFromClause("from user");
      provider.setWhereClause("where empId >= :fromId and empId <= :toId");
      provider.setSortKey("id");
      System.out.println("queryProvider end ");
      try {
        return provider.getObject();
      } catch (Exception e) {
        System.out.println("queryProvider exception ");
        e.printStackTrace();
      }
   
      return null;
    }

    @Bean
    @StepScope
    ItemProcessor<Employee, Employee> employeeItemProcessor(@Value("#{stepExecutionContext[name]}") String name) {
        return new ItemProcessor<Employee, Employee>() {
            @Override
            public Employee process(Employee employee) throws Exception {
                employee.setFirstName(employee.getFirstName().toUpperCase());
                employee.setLastName(employee.getLastName().toUpperCase());
                return employee;
            }
        };
    }
    
    @Bean
    @StepScope
    ItemWriter<Employee> employeeItemWriter(@Value("#{stepExecutionContext[fromId]}") final String fromId,
    	      @Value("#{stepExecutionContext[toId]}") final String toId) {
        return new ItemWriter<Employee>() {
            @Override
            public void write(List<? extends Employee> employeesList) throws Exception {
                for (Employee employee : employeesList) {
                    System.out.println("Name: "
                            + employee.getFirstName() + " "
                            + employee.getLastName() + "; "
                            + "Age: " + employee.getAge() + "; "
                            + "Salary: " + employee.getSalary());
                    repo.save(employee);
                }
            }
        };
    }

}

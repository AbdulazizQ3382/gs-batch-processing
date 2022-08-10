package com.example.batchprocessing;

import javax.sql.DataSource;

import com.example.batchprocessing.tasklets.FileCreatingTasklet;
import com.example.batchprocessing.tasklets.FileDeletingTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;

import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

// tag::setup[]
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;



	// end::setup[]

	// tag::readerwriterprocessor[]
	@Bean
	public FlatFileItemReader<Person> reader() {
		return new FlatFileItemReaderBuilder<Person>()
				.name("personItemReader")
				.resource(new ClassPathResource("flatFiles/sample-data1.csv"))
				.delimited()
				.names(new String[]{"firstName", "lastName"})
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
					setTargetType(Person.class);
				}})
				.build();
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Person>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
				.dataSource(dataSource)
				.build();
	}
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1, JdbcBatchItemWriter<Person> writer,DataSource dataSource) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(step1)
				.next(step2(writer(dataSource)))
				.next(step3())
				.end()
				.build();
	}


	@Bean
	public Step step1() {
		FileCreatingTasklet task = new FileCreatingTasklet();
		return stepBuilderFactory.get("step1")
				.tasklet(task)
				.build();
	}

	@Bean
	public Step step2(JdbcBatchItemWriter<Person> writer) {
		return stepBuilderFactory.get("step2")
				.<Person, Person>chunk(10)
				.reader(reader())
				.processor(processor())
				.writer(writer)
				.build();
	}


	@Bean
	public Step step3() {
		FileDeletingTasklet task = new FileDeletingTasklet();
		task.setResources();
		return stepBuilderFactory.get("step3")
				.tasklet(task)
				.build();
	}
}


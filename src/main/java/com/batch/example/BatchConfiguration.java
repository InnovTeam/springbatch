package com.batch.example;

import java.util.Arrays;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.data.builder.MongoItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.batch.example.dto.Person;
import com.mongodb.MongoClientURI;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration extends DefaultBatchConfigurer {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	@Autowired
	private MongoTemplate mongoTemplate;

	// tag::readerwriterprocessor[]
	@Bean
	public FlatFileItemReader<Person> reader() {
		return new FlatFileItemReaderBuilder<Person>().name("personItemReader")
				.resource(new ClassPathResource("sampletab.txt")).lineMapper(lineMapper())
				//.names(new String[] { "firstName", "lastName" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
					{
						setTargetType(Person.class);
					}
				}).build();
	}
	
	@Bean
	public DefaultLineMapper<Person> lineMapper(){
	      DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
	      lineMapper.setLineTokenizer(lineTokenizer());
	      lineMapper.setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
	                {
	                    setTargetType(Person.class);
	                }
	            });
	      return lineMapper;
	}

	@Bean
	public DelimitedLineTokenizer lineTokenizer() {
	    DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
	    tokenizer.setDelimiter("|");
	    tokenizer.setNames(new String[] { "firstName", "lastName" });
	    return tokenizer;
	}

	@Bean
	public ItemProcessor<Person, Person> processor() {
		final CompositeItemProcessor<Person, Person> processor = new CompositeItemProcessor<>();
	    processor.setDelegates(Arrays.asList(new PersonItemProcessor(), new TransactionValidatingProcessor()));
		return processor;
	}

	@Bean
	public MongoItemWriter<Person> writer() {
		return new MongoItemWriterBuilder<Person>().template(mongoTemplate).build();

	}

	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob(/* JobCompletionNotificationListener listener, */Step step1) {
		return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer())//
				//.listener(listener)
				.flow(step1)
				.end().build();
	}

	@Bean
	public Step step1(MongoItemWriter<Person> writer) {
		return stepBuilderFactory.get("step1").<Person, Person>chunk(10).reader(reader()).processor(processor())
				.writer(writer).build();
	}
	// end::jobstep[]

	@Bean
	@Autowired
	public MongoDbFactory mongoDbFactory(MongoConfiguration configuration) throws Exception {
		MongoClientURI mongoClient = new MongoClientURI(configuration.getUri());
		return new SimpleMongoDbFactory(mongoClient);
	}

	@Bean
	@Autowired
	public MongoTemplate mongoTemplate(MongoDbFactory mongoDbFactory) throws Exception {
		MongoTemplate mongoTemplate = new MongoTemplate(mongoDbFactory);
		return mongoTemplate;
	}
	@Override
    public void setDataSource(DataSource dataSource) {
        // override to do not set datasource even if a datasource exist.
        // initialize will use a Map based JobRepository (instead of database)
    }
}
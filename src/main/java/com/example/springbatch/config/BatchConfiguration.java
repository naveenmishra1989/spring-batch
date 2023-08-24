package com.example.springbatch.config;

import com.example.springbatch.dto.User;
import com.example.springbatch.dto.repo.UserRepository;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.persistence.EntityManagerFactory;
import java.util.concurrent.TimeUnit;

@Log4j2
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private EntityManagerFactory emf;

    @Value("classPath:sample-data.csv")
    private Resource inputResource;
    @Bean
    public FlatFileItemReader<User> reader ( ) {
        FlatFileItemReader reader = new FlatFileItemReader<>();
        reader.setResource(inputResource);
        reader.setLinesToSkip(1);

        DefaultLineMapper lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("FirstName", "LastName");

        BeanWrapperFieldSetMapper fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(User.class);

        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(tokenizer);
        reader.setLineMapper(lineMapper);

        return reader;
    }

    @Bean
    public JpaItemWriter writer ( ) {
        JpaItemWriter writer = new JpaItemWriter();
        writer.setEntityManagerFactory(emf);
        return writer;
    }

    @Bean
    public ItemProcessor<User, User> processor ( ) {
        return (item) -> {
            User user = User.builder().lastName(item.getLastName().toLowerCase())
                    .firstName(item.getFirstName().toUpperCase())
                    .build();
            return user;
        };
    }

    @Bean
    public Job importUserJob (JobExecutionListener listener) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1 ( ) {
        return stepBuilderFactory.get("step1")
                .<User, User>chunk(2)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public JobExecutionListener listener ( ) {
        return new JobExecutionListener() {


            @Override
            public void beforeJob (JobExecution jobExecution) {
                log.info(" JOB STARTED ! ");
            }

            @Override
            public void afterJob (JobExecution jobExecution) {
                if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                    log.info("!!! JOB FINISHED! Time to verify the results Inserted: "+userRepository.count());
                    userRepository.findAll().
                            forEach(person -> log.info("Found <" + person + "> in the database. "));
                }
            }
        };
    }
}

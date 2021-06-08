package com.example.sbremote.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class CustomerBatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Autowired
    @Qualifier("masterStep")
    private Step masterStep;

    @Bean
    public Job accountJob() {
        return this.jobBuilderFactory.get("account-job")
                .incrementer(new RunIdIncrementer())
                .start(processFileStep())
                .next(masterStep)
                .build();
    }

    @Bean
    public Step processFileStep() {
        return this.stepBuilderFactory.get("file-step")
                .<Customer, Customer>chunk(10)
                .reader(itemReader())
                .processor(new ItemProcessor<Customer, Customer>() {
                    @Override
                    public Customer process(Customer customer) throws Exception {
                        return customer;
                    }
                })
                .writer(itemWriter())
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<Customer> itemReader() {
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);
        H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();
        queryProvider.setSelectClause("SELECT id, name");
        queryProvider.setFromClause("FROM customers");
        queryProvider.setSortKeys(sortKeys);
//        return new JdbcPagingItemReaderBuilder<Customer>()
//                .name("pagingItemReader")
//                .dataSource(dataSource)
//                .pageSize(10)
//                .queryProvider(queryProvider)
//                .rowMapper(new BeanPropertyRowMapper<>(Customer.class))
//                .build();
        JdbcPagingItemReader<Customer> jdbcPagingItemReader = new JdbcPagingItemReader<>();
        jdbcPagingItemReader.setDataSource(dataSource);
        jdbcPagingItemReader.setFetchSize(10);
        jdbcPagingItemReader.setQueryProvider(queryProvider);
        jdbcPagingItemReader.setRowMapper(new BeanPropertyRowMapper<>(Customer.class));
        return  jdbcPagingItemReader;
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Customer> itemWriter() {
//        return new JdbcBatchItemWriterBuilder<Customer>()
//                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Customer>())
//                .sql("UPDATE customers set name=:name where id=:id")
//                .dataSource(dataSource)
//                .build();
        JdbcBatchItemWriter<Customer> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Customer>());
//        jdbcBatchItemWriter.setItemPreparedStatementSetter(((customer, ps) -> {
//            ps.setString(1, customer.getName());
//            ps.setLong(2, customer.getId());
//        }));
        jdbcBatchItemWriter.setSql("UPDATE customers set name=:name where id=:id");
        jdbcBatchItemWriter.setDataSource(dataSource);
        return jdbcBatchItemWriter;
    }
}

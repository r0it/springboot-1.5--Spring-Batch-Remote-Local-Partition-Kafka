package com.example.sbremote.batch;

import com.example.sbremote.batch.partitioner.LocalPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.BeanFactoryStepLocator;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.kafka.Kafka;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.support.PeriodicTrigger;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
public class WorkerStepConfiguation {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobExplorer jobExplorer;

    @Autowired
    DataSource dataSource;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    private ItemReader<Customer> crItemReader;

    @Autowired
    private ItemWriter<Customer> crItemWriter;

    @Autowired
    private LocalPartitioner localPartitioner;

    @Autowired
    private RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory;

    @Autowired
    public JobRepository jobRepository;

    @Autowired
    public JsonToStepExecutionRequestTransformer jsonToStepExecutionRequestTransformer;

    @Bean("crItemReader")
    @StepScope
    public ItemReader<Customer> reader(@Value("#{stepExecutionContext['locStartIdx']}") Long startIdx,
                                       @Value("#{stepExecutionContext['locEndIdx']}") Long endIdx){
        log.info("Local Reader- locStartIdx: {}, locEndIdx: {}", startIdx, endIdx);
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);
        H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();
        queryProvider.setSelectClause("SELECT id, name");
        queryProvider.setFromClause("FROM customers");
        queryProvider.setWhereClause("id >= " + startIdx + " and id <= " + endIdx);
        queryProvider.setSortKeys(sortKeys);

        JdbcPagingItemReader<Customer> jdbcPagingItemReader = new JdbcPagingItemReader<>();
        jdbcPagingItemReader.setDataSource(dataSource);
        jdbcPagingItemReader.setFetchSize(10);
        jdbcPagingItemReader.setQueryProvider(queryProvider);
        jdbcPagingItemReader.setRowMapper(new BeanPropertyRowMapper<>(Customer.class));
        return  jdbcPagingItemReader;
    }

    @Bean("crItemWriter")
    @StepScope
    public JdbcBatchItemWriter<Customer> itemWriter() {
        JdbcBatchItemWriter<Customer> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Customer>());
        jdbcBatchItemWriter.setSql("UPDATE customers set name=:name where id=:id");
        jdbcBatchItemWriter.setDataSource(dataSource);
        return jdbcBatchItemWriter;
    }

    @Bean
    public ThreadPoolTaskExecutor workerTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);
        executor.setQueueCapacity(20);
        executor.setMaxPoolSize(20);
        executor.setThreadNamePrefix("sb-worker-");
        executor.initialize();

        return executor;
    }

    /*
     * Configure inbound flow (requests coming from the master)
     */
    @Bean
    public DirectChannel workerRequests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow inboundFlow(KafkaProperties kafkaProperties, PollableChannel replies) throws Exception {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put("spring.json.trusted.packages", "*");
        return IntegrationFlows
                .from(Kafka.messageDrivenChannelAdapter(new DefaultKafkaConsumerFactory(props, new
                        StringDeserializer(), new StringDeserializer()) , "migration-topics"))
                .transform(jsonToStepExecutionRequestTransformer)
                .handle(stepExecutionRequestHandler())
                .channel(replies)
                .get();
    }

    @Bean
    public Step remoteWorkerStep() throws Exception {
        return stepBuilderFactory.get("remoteWorkerStep"
                +System.currentTimeMillis() //temporary fix for JobExecutionException: Cannot restart step from STARTED status
        )
                .allowStartIfComplete(true)
                .listener(localPartitioner)
                .partitioner("localWorkerStep", localPartitioner)
                .step(localWorkerStep())
                .gridSize(4)
                .taskExecutor(workerTaskExecutor())
                .build();
    }

    @Bean
    public DirectChannel workerReplies(){
        return new DirectChannel();
    }

    @Bean
    public StepExecutionRequestHandler stepExecutionRequestHandler(){
        StepExecutionRequestHandler stepExecutionRequestHandler = new StepExecutionRequestHandler();
        BeanFactoryStepLocator stepLocator = new BeanFactoryStepLocator();
        stepLocator.setBeanFactory(this.applicationContext);
        stepExecutionRequestHandler.setStepLocator(stepLocator);
        stepExecutionRequestHandler.setJobExplorer(this.jobExplorer);

        return stepExecutionRequestHandler;
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller()
    {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(10));
        return pollerMetadata;
    }

    @Bean
    public Step localWorkerStep() {
        return stepBuilderFactory.get("localWorkerStep"
                +System.currentTimeMillis() //temporary fix for JobExecutionException: Cannot restart step from STARTED status
        )
                .allowStartIfComplete(true)
                .<Customer, Customer>chunk(10)
                .reader(crItemReader)
                .processor(new ItemProcessor<Customer, Customer>() {
                    @Override
                    public Customer process(Customer customer) throws Exception {
                        log.info("id: {}", customer.getId());
                        return customer;
                    }
                })
                .writer(crItemWriter)
                .build();
    }
}

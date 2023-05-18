package com.empowerretirement.moneyout.batch;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.mail.internet.MimeMessage;
//import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.mail.javamail.MimeMessageHelper;
//import com.gwf.common.datasource.RoutingDataSource;
import org.springframework.transaction.PlatformTransactionManager;

import com.empowerretirement.moneyout.listener.IncomeFlexDepletionReportJobListener;
import com.empowerretirement.moneyout.model.MyObject;
import com.gwf.common.datasource.RoutingDataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
	public RoutingDataSource dataSource;
    
    @Autowired
    private PlatformTransactionManager transactionManager;
    
    @Autowired
    private org.springframework.mail.javamail.JavaMailSender javaMailSender;

    @Value("${notification.email.to}")
    private String notificationEmailTo;    

    @Value("${dbInstance:#{null}}")
	private String databaseName;
    
    private static final String QUERY_FIND_LOCKIN_PPT = "select ga_id, ind_id, sdio_id,prod_subtype from glwb_inv_rider_acct_view where PROD_TYPE='INCOMEFLEX' and status_code='P'";

    @Bean
    @Qualifier("Step1")
    @StepScope
    public JdbcCursorItemReader<MyObject> reader() {
        JdbcCursorItemReader<MyObject> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource.getDataSource(databaseName));
        reader.setSql(QUERY_FIND_LOCKIN_PPT );
        reader.setRowMapper(new MyObjectRowMapper());
        return reader;
    }

    public class MyObjectRowMapper implements RowMapper<MyObject> {
        @Override
        public MyObject mapRow(ResultSet rs, int rowNum) throws SQLException {
            MyObject data=new MyObject();
            data.setGa_id(rs.getInt("ga_id"));
            data.setInd_id(rs.getInt("ind_id"));
            data.setSdio_id(rs.getInt("stdio_id"));
            data.setProd_subtype(rs.getString("prod_subtype"));
            return data;
        } 
    }

    @Bean
    public FlatFileItemWriter<MyObject> writer() {
        FlatFileItemWriter<MyObject> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("IncomeFlexDepletionReport.csv"));
        writer.setHeaderCallback(writer1 -> writer1.write("GA ID, IND ID, STDIO ID, PROD SUBTYPE"));
        writer.setLineAggregator(new DelimitedLineAggregator<MyObject>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<MyObject>() {{
                setNames(new String[] {"ga_id","ind_id","stdio_id","prod_subtype"});
            }});
        }});
        return writer;
    }
    
    @Bean
    public JobExecutionListener incomeFlexDepletionReportJobListener() {
    	return new IncomeFlexDepletionReportJobListener();
    }

//    @Bean
//    public ItemProcessor<MyObject, Participant> processor() {
//        return new ParticipantProcessor(depletionThreshold);
//    }

    @Bean
    @Primary
    public Step step1() {
        return stepBuilderFactory.get("step1")
        .<MyObject, MyObject>chunk(10)
        .reader(reader())
        .writer(writer())
        .build();
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, JobExecutionListener incomeFlexDepletionReportJobListener,
			Step Step1, Step createNotificationStep) {
        return jobBuilderFactory.get("job")
        .incrementer(new RunIdIncrementer())
        .listener(incomeFlexDepletionReportJobListener)
        .flow(Step1)
        .next(createNotificationStep)
        .end()
        .build();
    }

    @Bean
    @Qualifier("createNotificationStep")
    @StepScope
    public Tasklet createNotificationTasklet() {
        return (contribution, chunkContext) -> {
            MimeMessage message = javaMailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true);
            helper.setTo(notificationEmailTo);
            helper.setSubject("Test");
            helper.setText("Please find the attached");
            FileSystemResource file = new FileSystemResource("IncomeFlexDepletionReport.csv");
            helper.addAttachment("IncomeFlexDepletionReport.csv", file);
            javaMailSender.send(message);
            return RepeatStatus.FINISHED;
        };
    }

    @Bean 
    public Step createNotificationStep() {
        return stepBuilderFactory.get("createNotificationStep")
        .tasklet(createNotificationTasklet())
        .build();
    }
    
    //@Override
	public JobRepository getJobRepository(){
		JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
		factory.setDataSource(dataSource.getDataSource("sb"));
		factory.setTransactionManager(transactionManager);
		factory.setIsolationLevelForCreate("ISOLATION_READ_COMMITTED");
		factory.setTablePrefix("BATCH_");
		JobRepository jobRepository = null;
		try {
			 jobRepository = factory.getObject();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jobRepository;
	}


	//@Override
	public JobLauncher getJobLauncher(){
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(getJobRepository());
		try {
			jobLauncher.afterPropertiesSet();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jobLauncher;
	}
}



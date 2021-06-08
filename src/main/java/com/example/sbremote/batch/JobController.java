package com.example.sbremote.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@RestController
public class JobController {

    @Autowired
    Job accountJob;

    @Autowired
    JobLauncher jobLauncher;

    @GetMapping("/")
    public String findById() throws Exception {
        Date date = new Date();
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addDate("date", date);
        jobLauncher.run(accountJob, jobParametersBuilder.toJobParameters());
        return "Job Launched with date: " + date.toString();
    }

}
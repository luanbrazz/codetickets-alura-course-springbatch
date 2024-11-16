package com.alura.codetickets;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ImportacaoJobConfiguration {

    public Job job(Step passoInicial, JobRepository jobRepository){
        return new JobBuilder("geracao-tickets", jobRepository)
                .start(passoInicial)
                .incrementer(new RunIdIncrementer())
                .build();
    }
}

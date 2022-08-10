package com.example.batchprocessing.tasklets;

import java.io.File;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;

public class FileDeletingTasklet implements Tasklet, InitializingBean {

    private File[] files;
    @Value("file:C:\\Users\\aqannam\\IdeaProjects\\gs-batch-processing\\complete\\src\\main\\resources\\flatFiles\\sample-data.csv")


    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        System.out.println("hello");
        System.out.println(files.length);
        for(File f: files) {

            boolean deleted = f.delete();
            if (!deleted) {
                throw new UnexpectedJobExecutionException("Could not delete file " + f.getPath());
            }
        }
        return RepeatStatus.FINISHED;
    }

    public void setResources() {
        File folder = new File("C:\\Users\\aqannam\\IdeaProjects\\gs-batch-processing\\complete\\src\\main\\resources\\flatFiles");
        this.files = folder.listFiles();
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(files, "directory must be set");
    }
}
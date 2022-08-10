package com.example.batchprocessing.tasklets;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;

import java.io.*;
import java.util.List;

public class FileCreatingTasklet implements Tasklet, InitializingBean {




    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        File file = new File("C:\\Users\\aqannam\\IdeaProjects\\gs-batch-processing\\complete\\src\\main\\resources\\flatFiles\\sample-data1.csv");
        try {
            // create FileWriter object with file as parameter
            FileWriter outputFile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputFile);
            int lineFrom = (int)Math.floor(Math.random()*4000);
            // adding header to csv
            List<String[]> names = readDataLineByLine("C:\\Users\\aqannam\\IdeaProjects\\gs-batch-processing\\complete\\src\\main\\resources\\source\\names.csv");

                for(int i = lineFrom; i<names.size()-1;i++)
                writer.writeNext(names.get(i));


            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return RepeatStatus.FINISHED;
    }

    public static List<String[]> readDataLineByLine(String file) throws IOException, CsvException {

            // Create an object of filereader
            // class with CSV file as a parameter.
            FileReader filereader = new FileReader(file);

            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader);
        return csvReader.readAll();
    }

    public void afterPropertiesSet() throws Exception {
    }
}

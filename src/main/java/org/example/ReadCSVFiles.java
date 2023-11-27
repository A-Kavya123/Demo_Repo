package org.example;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;


public class ReadCSVFiles {

    public static void readCSV() {

        String accessKey = "AKIARNIMKT2NW4IW6NRA";
        String secretKey = "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";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withRegion(Regions.AP_SOUTH_1.getName())
                .build();

        String sourceBucketName =LambdaHandler.bucketName;
       // String sourcePrefix = "CSVData_Flow/27fcd3d1-40bc-413d-8d4c-26479e4186c1/";
        String sourcePrefix = LambdaHandler.folderName;
        String destinationBucketName = LambdaHandler.bucketName;
        String destinationKey = "Workday_CSV/combined.csv";
        //String destinationKey = "Workday_CSV/combined2.csv";

        // List objects in the source bucket with the specified prefix
        ObjectListing listObjects;
        List<String[]> records = new ArrayList<>();
        do {
            listObjects = s3Client.listObjects(sourceBucketName, sourcePrefix);
              //A list of the object summaries describing the objects stored in the S3 bucket.
            List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
            for (S3ObjectSummary objectSummary : objectSummaries) {
               System.out.println("<<<<<<<<<<<<<<<object summery"+objectSummary);
                S3Object s3Object = s3Client.getObject(sourceBucketName, objectSummary.getKey());//Multiple_CSV
                //System.out.println("s3 object"+s3Object);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8));
                     CSVReader csvReader = new CSVReader(reader)) {
                    String[] fileRecords;
                    while ((fileRecords = csvReader.readNext()) != null) {
                        records.add(fileRecords);
                    }
                } catch (IOException | CsvValidationException e) {
                    e.printStackTrace();
                }
            }
        } while (listObjects.isTruncated());
        // Write the combined records to a new CSV file
        //writing characters to the ByteArrayOutputStream and converts them to bytes using the UTF-8 character encoding.
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
             CSVWriter csvWriter = new CSVWriter(writer)) {

            csvWriter.writeAll(records);
            csvWriter.flush();
            byte[] csvBytes = outputStream.toByteArray();

            // Upload the combined CSV file to the destination bucket
            InputStream csvInputStream = new ByteArrayInputStream(csvBytes);//reads the data from byte array
            System.out.println("<<<<<<<<<<<<csv input stream"+csvInputStream);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(csvBytes.length);
            s3Client.putObject(new PutObjectRequest(destinationBucketName, destinationKey, csvInputStream, metadata));

            System.out.println("<<<<<<Combined CSV file uploaded to S3: " + destinationBucketName + "/" + destinationKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}






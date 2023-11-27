package org.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.json.JSONObject;
import org.json.simple.JSONArray;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Csvtojson {
   // public static void readCsvtoJson() {
    public static void main(String[] args) {



            // Your existing code for reading CSV file
            String accessKey = "AKIARNIMKT2NW4IW6NRA";
            String secretKey = "dKDC1XK5wAwWjk72XdD3ITD34ThwRrX4nP/vCkd/";

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withRegion(Regions.AP_SOUTH_1.getName())
                    .build();

//              String sourceBucketName =LambdaHandler.bucketName;
//        String sourcePrefix = LambdaHandler.folderName;
//         String destinationBucketName = LambdaHandler.bucketName;

    String sourceBucketName ="appflow-connectors-data";
         String sourcePrefix = "CSVData_Flow/f7c8d5e6-2064-4606-b295-a376ee0582af/";

        String destinationBucketName = "appflow-connectors-data";
            String destinationKey = "Workday_CSV/combined7.json";

        ObjectListing listObjects;
        List<String[]> records = new ArrayList<>();
        do {
            listObjects = s3Client.listObjects(sourceBucketName, sourcePrefix);
            List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
            for (S3ObjectSummary objectSummary : objectSummaries) {
                S3Object s3Object = s3Client.getObject(sourceBucketName, objectSummary.getKey());
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

        // Converting CSV records to JSON
        if (records.size() > 1) {
            JSONArray jsonArray = new JSONArray();
            String[] headers = records.get(0);

            // Identify columns with data
            List<Integer> columnsWithData = new ArrayList<>();
            for (int j = 0; j < headers.length; j++) {
                boolean hasDataInColumn = false;
                for (int i = 1; i < records.size(); i++) {
                    String[] record = records.get(i);
                    if (j < record.length && !record[j].isEmpty()) {
                        hasDataInColumn = true;
                        break;
                    }
                }
                if (hasDataInColumn) {
                    columnsWithData.add(j);
                }
            }

            for (int i = 1; i < records.size(); i++) {
                String[] record = records.get(i);

                JSONObject jsonObject = new JSONObject();
                for (int column : columnsWithData) {
                    if (column < record.length) {
                        String header = headers[column];
                        String value = record[column];

                        // Skip fields without data
                        if (!value.isEmpty()) {
                            jsonObject.put(header, value);
                        }
                    }
                }
                jsonArray.add(jsonObject);
            }
            // Convert JSON array to a string
            String jsonString = jsonArray.toJSONString();

            // Upload the JSON string to the destination bucket
            try (InputStream inputStream = new ByteArrayInputStream(jsonString.getBytes())) {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(jsonString.length());
                s3Client.putObject(new PutObjectRequest(destinationBucketName, destinationKey, inputStream, metadata));
                System.out.println("<<<<<<JSON file uploaded to S3: " + destinationBucketName + "/" + destinationKey);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("<<<<No records found in the CSV files.");
        }
    }
}


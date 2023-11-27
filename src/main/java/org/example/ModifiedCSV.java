package org.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ModifiedCSV {

    private static final String CSV_SEPARATOR = ",";
   // public static void readCSV() throws IOException {
    public static void main(String[] args) throws IOException {
        String accessKey = "AKIARNIMKT2NW4IW6NRA";
        String secretKey = "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";

//        String sourceBucketName =LambdaHandler.bucketName;
//        String sourcePrefix = LambdaHandler.folderName;
//        String destinationBucketName = LambdaHandler.bucketName;

        String sourceBucketName = "appflow-connectors-data";
        String sourcePrefix = "CSVData_Flow/e466e85d-ea58-4ec6-971b-70eab2d83181/";
        String destinationBucketName = "appflow-connectors-data";
        String destinationKeyPrefix = "Workday_CSV/combined507";
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("s3.amazonaws.com", "ap-south-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();

        List<String[]> records = new ArrayList<>();
        List<S3ObjectSummary> s3Objects = new ArrayList<>();

        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(sourceBucketName)
                .withPrefix(sourcePrefix);

        ListObjectsV2Result listObjectsResponse;
        do {
            listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
            List<S3ObjectSummary> objectSummaries = listObjectsResponse.getObjectSummaries();
            System.out.printf("<<<<<<<object summaries"+objectSummaries);
            for (S3ObjectSummary objectSummary : objectSummaries) {
                s3Objects.add(objectSummary);
            }
            listObjectsRequest.setContinuationToken(listObjectsResponse.getNextContinuationToken());
        } while (listObjectsResponse.isTruncated());

        for (S3ObjectSummary s3Object : s3Objects) {
            GetObjectRequest getObjectRequest = new GetObjectRequest(sourceBucketName, s3Object.getKey());
            System.out.println("<<<<<<<<<<S3 OBJECT"+s3Object);
            try (S3ObjectInputStream s3ObjectStream = s3Client.getObject(getObjectRequest).getObjectContent()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(s3ObjectStream, StandardCharsets.UTF_8));
                CSVReader csvReader = new CSVReader(reader);

                String[] fileRecords;
                while ((fileRecords = csvReader.readNext()) != null) {
                    records.add(fileRecords);

                }
            } catch (IOException | CsvValidationException e) {
                e.printStackTrace();
            }
        }
        System.out.println("<<<<<<<<<<<records size"+records.size());
        if (records.size() > 1) {
            // Combine CSV data
            StringWriter combinedCsvWriter = new StringWriter();
            System.out.printf("<<<<<<<<combined csv "+combinedCsvWriter);
            for (String[] record : records) {
                writeLine(combinedCsvWriter, Arrays.asList(record));
            }
            String combinedCsvData = combinedCsvWriter.toString();

            // Convert CSV data to JSON
            JSONArray jsonArray = new JSONArray();
            System.out.printf("<<<<<<<<json arrray"+jsonArray);
            String[] headers = records.get(0);

            for (int i = 1; i < records.size(); i++) {
                String[] record = records.get(i);
                JSONObject jsonObject = new JSONObject();

                for (int j = 0; j < record.length; j++) {
                    if (j < headers.length) {
                        String header = headers[j];
                        String value = record[j];
                        if (!value.isEmpty()) {
                            try {
                                JSONObject valueJson = new JSONObject(value);
                                jsonObject.put(header, valueJson);
                            } catch (JSONException e) {
                                jsonObject.put(header, value);
                            }
                        }
                    }
                }

                jsonArray.put(jsonObject);
            }

            String jsonString = jsonArray.toString();

            // Upload JSON file to S3
            try (InputStream inputStream = new ByteArrayInputStream(jsonString.getBytes())) {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(jsonString.getBytes().length);

                PutObjectRequest putObjectRequest = new PutObjectRequest(destinationBucketName, destinationKeyPrefix, inputStream, metadata);
                PutObjectResult putObjectResult = s3Client.putObject(putObjectRequest);

                System.out.println("<<<<JSON file uploaded to S3: " + destinationBucketName + "/" + destinationKeyPrefix);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("<<<<<<No records found in the CSV files.");
        }
    }

    private static void writeLine(Writer writer, List<String> values) throws IOException {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            if (!first) {
                sb.append(CSV_SEPARATOR);
            }
            sb.append(value);
            first = false;
        }
        sb.append("\n");
        writer.append(sb.toString());
    }

}
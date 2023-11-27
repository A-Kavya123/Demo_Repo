package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;


public class LambdaHandler implements RequestHandler<S3Event, String> {

    public static String bucketName = "";
    public static String folderName = "";
    public static String validfilePath = "";
    public static String fileName = "";
    public static String s3Path = "";
    public String handleRequest(S3Event s3Event, Context context) {
        try {
            int i = 0;
            bucketName = s3Event.getRecords().get(0).getS3().getBucket().getName();
            String filePath = s3Event.getRecords().get(i).getS3().getObject().getKey();
            String replaced = filePath.replaceAll("[+]", " ");
            validfilePath = replaced.replaceAll("%3A", ":");
            int index = validfilePath.indexOf("/");
            folderName = validfilePath.substring(0, index); // "Folder name"
            fileName = validfilePath.substring(index + 1); // "File name"
            i++;
            System.out.println(">>>>> Lambda Got Triggered When File Uploads and Replaces<<<<<");
            System.out.println(
                    "S3 Path of Uploaded file >>>>>>>>>>>>>>>>>\t" + "s3://" + bucketName + "/" + validfilePath);
            System.out.println("Bucket name\t ----> " + bucketName);
            System.out.println("file path\t ---->  " + validfilePath);
            System.out.println("folder name\t ----> " + folderName);
            System.out.println("file name\t --->" + fileName);
            s3Path = "s3://" + bucketName + "/" + folderName + "/";

            //ModifiedCSV.readCSV();
           // ReadCSVFiles.readCSV();
            ConvertToNDJSON.readCSV();
        } catch (Exception e) {
            return "Error while reading file from s3" + e.getMessage();
        }
        return "Successfully fetching data from S3 bucket";
    }
}
























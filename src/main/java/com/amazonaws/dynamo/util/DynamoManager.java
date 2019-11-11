package com.amazonaws.dynamo.util;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

public class DynamoManager {

  private static AWSCredentialsProvider credentialsProvider =
      new AWSStaticCredentialsProvider(
          new BasicAWSCredentials(DynamoConstants.ACCESS_KEY, DynamoConstants.ACCESS_SECRET));

  private static AmazonDynamoDB dynamoDB =
      AmazonDynamoDBClientBuilder.standard().withCredentials(credentialsProvider).withRegion(DynamoConstants.REGION)
          .build();

  private static DynamoDB dynamo = new DynamoDB(dynamoDB);

  private static Table table = dynamo.getTable(DynamoConstants.TABLE_NAME);

  public static Table getTable() {
    return table;
  }

  public static AmazonDynamoDB getDynamoDB() {
    return dynamoDB;
  }

  public static DynamoDB getDynamo() {
    return dynamo;
  }

}

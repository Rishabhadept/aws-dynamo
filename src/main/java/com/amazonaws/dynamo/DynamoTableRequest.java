package com.amazonaws.dynamo;

import org.apache.log4j.Logger;

import com.amazonaws.dynamo.util.DynamoManager;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;

public class DynamoTableRequest {

  static final Logger logger = Logger.getLogger(DynamoTableRequest.class);

  /**
   * Create a new table with partition key and sort key if it does not exist
   * 
   * @param tableName
   * @param partitionKey
   * @param sortKey
   * @return String - Table name
   */
  public static String createTable(String tableName,String partitionKey,String sortKey){

    // Create a table with a primary hash key named 'name', which holds a string 
    CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName).withKeySchema(new
    KeySchemaElement().withAttributeName(partitionKey).withKeyType(KeyType.HASH))
        .withKeySchema(new KeySchemaElement().withAttributeName(sortKey).withKeyType(KeyType.RANGE))
        .withAttributeDefinitions(
            new AttributeDefinition().withAttributeName(partitionKey).withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition().withAttributeName(sortKey).withAttributeType(ScalarAttributeType.S))
        .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));

    // Create table if it does not exist yet
    TableUtils.createTableIfNotExists(DynamoManager.getDynamoDB(), createTableRequest);
    // wait for the table to move into ACTIVE state
    try {
      TableUtils.waitUntilActive(DynamoManager.getDynamoDB(), tableName);
    } catch (TableNeverTransitionedToStateException | InterruptedException e) {
      logger.error("Exception while creating table and waiting until active: ", e);
    }

    DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
    TableDescription tableDescription = DynamoManager.getDynamoDB().describeTable(describeTableRequest).getTable();
    return tableDescription.getTableName();
  }

}

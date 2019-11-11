package com.amazonaws.dynamo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.dynamo.util.DynamoConstants;
import com.amazonaws.dynamo.util.DynamoManager;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class DynamoPostRequest {

  static final Logger logger = Logger.getLogger(DynamoPostRequest.class);
  /**
   * Add data in table
   * 
   * @param partitionKeyValue - table Name
   * @param sortKeyValue - Unique key
   * @param dataMap - Map<String, Object> - all other key value pairs in form of a map
   */
  public static PutItemResult postData(String partitionKeyValue, String sortKeyValue, Map<String, Object> dataMap) {

    Item item = new Item()
        .withPrimaryKey(DynamoConstants.PARTITION_KEY, partitionKeyValue, DynamoConstants.SORT_KEY, sortKeyValue);

    dataMap.forEach((key, value) -> {
      item.with(key, value);
    });

    PutItemOutcome putItemOutcome = DynamoManager.getTable().putItem(item);

    return putItemOutcome.getPutItemResult();

  }

  /**
   * Add data in table
   * 
   * @param partitionKeyValue - table Name
   * @param sortKeyValue - Unique key
   * @param key - Key against which json will be saved
   * @param json - json data
   */
  public static PutItemResult postData(String partitionKeyValue, String sortKeyValue, String key, String json) {

    Item item = new Item()
        .withPrimaryKey(DynamoConstants.PARTITION_KEY, partitionKeyValue, DynamoConstants.SORT_KEY, sortKeyValue)
        .withJSON(key, json);

    DynamoManager.getTable().putItem(item);

    PutItemOutcome putItemOutcome = DynamoManager.getTable().putItem(item);

    return putItemOutcome.getPutItemResult();

  }

  /**
   * Add bulk within single table.
   * 
   * @reference https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/batch-operation-document-api-java.html#
   *            JavaDocumentAPIBatchWrite
   * 
   * @param partitionKeyValue - Table name
   * @param data - Key will be the sort key and value will contain map of all other key/value pair.
   */
  public static BatchWriteItemResult postBulkData(String partitionKeyValue, Map<String, Map<String, Object>> data) {

    Map<String, List<WriteRequest>> requestItems = new HashMap<>();
    List<WriteRequest> writeRequests = new ArrayList<>();

    data.forEach((key, value) -> {
      Map<String, AttributeValue> item = newItem(partitionKeyValue, key, value);
      PutRequest putRequest = new PutRequest(item);
      WriteRequest writeRequest = new WriteRequest(putRequest);

      writeRequests.add(writeRequest);
    });

    requestItems.put(DynamoConstants.TABLE_NAME, writeRequests);
    BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest(requestItems);

    BatchWriteItemResult batchWriteItemResult = DynamoManager.getDynamoDB().batchWriteItem(batchWriteItemRequest);
    BatchWriteItemOutcome outcome = new BatchWriteItemOutcome(batchWriteItemResult);

    do {
      // Check for unprocessed keys which could happen if you exceed
      // provisioned throughput

      Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

      if (outcome.getUnprocessedItems().size() == 0) {
        logger.debug("No unprocessed items found");
      } else {
        logger.debug("Retrieving the unprocessed items");
        outcome = DynamoManager.getDynamo().batchWriteItemUnprocessed(unprocessedItems);
      }

    } while (outcome.getUnprocessedItems().size() > 0);

    return outcome.getBatchWriteItemResult();
  }

  /**
   * Create new Item for bulk write operation
   * 
   * @param partitionKeyValue
   * @param sortKeyValue
   * @param dataMap - Input data
   * @return Map<String, AttributeValue>
   */
  private static Map<String, AttributeValue> newItem(String partitionKeyValue, String sortKeyValue,
      Map<String, Object> dataMap) {

    Map<String, AttributeValue> item = new HashMap<>();
    item.put(DynamoConstants.PARTITION_KEY, new AttributeValue(partitionKeyValue));
    item.put(DynamoConstants.SORT_KEY, new AttributeValue(sortKeyValue));

    dataMap.forEach((key, value) -> {
      item.put(key, new AttributeValue(value.toString()));
    });

    return item;
  }

}

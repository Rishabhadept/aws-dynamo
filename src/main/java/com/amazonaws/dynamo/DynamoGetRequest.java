package com.amazonaws.dynamo;

import java.util.HashMap;
import java.util.List;

import com.amazonaws.dynamo.util.DynamoConstants;
import com.amazonaws.dynamo.util.DynamoManager;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DynamoGetRequest {

  /**
   * Retrieve item based on partition key and sort key
   * 
   * @param partitionKeyValue
   * @param sortKeyValue
   * @return String - Json
   */
  public static String retrieveItem(String partitionKeyValue, String sortKeyValue) {

    Item item = DynamoManager.getTable().getItem(DynamoConstants.PARTITION_KEY, partitionKeyValue,
        DynamoConstants.SORT_KEY, sortKeyValue);

    return item.toJSONPretty();
  }

  /**
   * Retrieve item based on parameters key value
   * 
   * @param key - Key of the item
   * @param value
   * @return List<java.util.Map<String, AttributeValue>>
   */
  public static List<java.util.Map<String, AttributeValue>> scanItem(String key, String value) {

    HashMap<String, Condition> scanFilter = new HashMap<>();

    Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ.toString())
        .withAttributeValueList(new AttributeValue().withS(value));

    scanFilter.put(key, condition);
    ScanRequest scanRequest = new ScanRequest(DynamoConstants.TABLE_NAME).withScanFilter(scanFilter);
    ScanResult scanResult = DynamoManager.getDynamoDB().scan(scanRequest);
    return scanResult.getItems();
  }

}

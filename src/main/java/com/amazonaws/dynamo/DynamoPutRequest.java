package com.amazonaws.dynamo;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.amazonaws.dynamo.util.DynamoConstants;
import com.amazonaws.dynamo.util.DynamoManager;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

public class DynamoPutRequest {

  /**
   * Update multiple keys for a record
   * 
   * @param partitionKeyValue - table name
   * @param sortKeyValue - Unique Key
   * @param dataMap - Map with all the key/value pair which needs to be updated
   * @return - Updated json record
   */
  public static String updateMultipleAttributes(String partitionKeyValue, String sortKeyValue,
      Map<String, Object> dataMap) {

    StringBuilder sb = new StringBuilder("set");
    NameMap nameMap = new NameMap();
    ValueMap valueMap = new ValueMap();
    Set<String> keys = dataMap.keySet();
    Iterator<String> iterator = keys.iterator();

    for (int i = 0; i < dataMap.size(); i++) {
      String key = iterator.next();
      // create update expression:
      sb.append(" #key" + i + "=:val" + i);
      // create name Map
      nameMap.with("#key" + i, key);
      // create value map
      valueMap.with(":val" + i, dataMap.get(key));

      if (i != dataMap.size() - 1) {
        sb.append(",");
      }
    }

    UpdateItemSpec updateItemSpec = new UpdateItemSpec()
        .withPrimaryKey(DynamoConstants.PARTITION_KEY, partitionKeyValue, DynamoConstants.SORT_KEY, sortKeyValue)
        .withUpdateExpression(sb.toString())
        .withNameMap(nameMap)
        .withValueMap(valueMap)
        .withReturnValues(ReturnValue.ALL_NEW);

    UpdateItemOutcome outcome = DynamoManager.getTable().updateItem(updateItemSpec);

    // Check the response.
    return outcome.getItem().toJSONPretty();

  }

  /**
   * Update specific key value based on a single conditional key value
   * 
   * @param partitionKeyValue - table name
   * @param sortKeyValue - unique key
   * @param conditionKey - conditional key
   * @param conditionValue - conditional Value
   * @param updateKey - Key which needs to be updated
   * @param updateValue - Value for the key which needs to be updated.
   * @return String - Updated Record
   */
  public static String updateExistingAttributeConditionally(String partitionKeyValue, String sortKeyValue,
      String conditionKey, Object conditionValue, String updateKey, Object updateValue) {

    UpdateItemSpec updateItemSpec = new UpdateItemSpec()
        .withPrimaryKey(DynamoConstants.PARTITION_KEY, partitionKeyValue, DynamoConstants.SORT_KEY, sortKeyValue)
        .withReturnValues(ReturnValue.ALL_NEW).withUpdateExpression("set #p = :val1")
        .withConditionExpression("#p2 = :val2")
        .withNameMap(new NameMap().with("#p", updateKey).with("#p2", conditionKey))
        .withValueMap(new ValueMap().with(":val1", updateValue).with(":val2", conditionValue));

    UpdateItemOutcome outcome = DynamoManager.getTable().updateItem(updateItemSpec);

    // Check the response.
    return outcome.getItem().toJSONPretty();

  }

}

package com.amazonaws.dynamo;

import com.amazonaws.dynamo.util.DynamoConstants;
import com.amazonaws.dynamo.util.DynamoManager;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

public class DynamoDeleteRequest {

  /**
   * Delete item based on partition key and sort key
   * 
   * @param partitionKeyValue
   * @param sortKeyValue
   * @return String - If delete then it will return proper json output else it will return null
   */
  public static String deleteItem(String partitionKeyValue, String sortKeyValue) {

    DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
        .withPrimaryKey(DynamoConstants.PARTITION_KEY, partitionKeyValue, DynamoConstants.SORT_KEY, sortKeyValue)
        .withReturnValues(ReturnValue.ALL_OLD);

    DeleteItemOutcome outcome = DynamoManager.getTable().deleteItem(deleteItemSpec);

    if (outcome.getItem() != null) {
      return outcome.getItem().toJSONPretty();
    }
    return null;
  }

  /**
   * Delete item based on partition key and sort key. Additionally any other key with value can be passed to delete
   * conditionally only if the value matches.
   * 
   * @param partitionKeyValue
   * @param sortKeyValue
   * @param conditionKey
   * @param conditionValue
   * @return String
   */
  public static String deleteItemConditional(String partitionKeyValue, String sortKeyValue, String conditionKey,
      Object conditionValue) {

    DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
        .withPrimaryKey(DynamoConstants.PARTITION_KEY, partitionKeyValue, DynamoConstants.SORT_KEY, sortKeyValue)
        .withConditionExpression("#key = :val").withNameMap(new NameMap().with("#key", conditionKey))
        .withValueMap(new ValueMap().with(":val", conditionValue)).withReturnValues(ReturnValue.ALL_OLD);

    DeleteItemOutcome outcome = DynamoManager.getTable().deleteItem(deleteItemSpec);

    if (outcome.getItem() != null) {
      return outcome.getItem().toJSONPretty();
    }
    return null;
  }

}

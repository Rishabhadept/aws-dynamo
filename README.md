# aws-dynamo

The aws-dynamo project in Java provides a wrapper package on top of AWS libraries so that you need not to worry about understanding AWS classes and functions and investing your time on how to implement them.

A typical use of this library is when You want perform operations on your dynamo table based on partition key and sort key. since this is a generic library, it will even support the case where you have a single dynamo table with different type of data based on partition key and sort key.

It supports the following operations:
### 1. Create Table

```
	a. createTable(String tableName,String partitionKey,String sortKey) 
		~~ Create a new table with partition key and sort key if it does not exist.
```

### 2. Insert data in Table

```
	a. postData(String partitionKeyValue, String sortKeyValue, Map<String, Object> dataMap)
		 ~~ Add data in table with partitionKeyValue, sortKeyValue and  all other key value pairs in form of a map.
		
	b. postData(String partitionKeyValue, String sortKeyValue, String key, String json)
		~~ Add data in table with partitionKeyValue, sortKeyValue and a key against which json will be saved.
		
	c. postBulkData(String partitionKeyValue, Map<String, Map<String, Object>> data)
		~~ Add bulk data within single table. The Map will have key-value pair where key is sortKey and value will be a map of all other key-value pair.
```

### 3.	Update Attributes in the table	

```
	a. updateMultipleAttributes(String partitionKeyValue, String sortKeyValue, Map<String, Object> dataMap)
		~~ Update multiple keys for a record. dataMap param - Map with all the key/value pair that needs to be updated.
		
	b. updateExistingAttributeConditionally(String partitionKeyValue, String sortKeyValue,String conditionKey, Object conditionValue, String 		  updateKey, Object updateValue)
		~~ Update specific key value based on a single conditional key value.
```
		
### 4. Retrieve data

```
	a. String retrieveItem(String partitionKeyValue, String sortKeyValue)
		~~ Retrieve item based on partition key and sort key.
		
	b. scanItem(String key, String value)
		~~ Retrieve item based on parameters key value.
```
		
### 5. Delete data

```
	a. deleteItem(String partitionKeyValue, String sortKeyValue)
		~~ Delete item based on partition key and sort key.
		
	b. deleteItemConditional(String partitionKeyValue, String sortKeyValue, String conditionKey, Object conditionValue)
		~~ Delete item based on partition key and sort key. Additionally any other key with value can be passed to delete conditionally only if the value matches.
```

**Important**: DynamoConstants class has all the constants which are required to run this library. Replace all place holders here with appropriate values.

**Note**: Currently, the table name is hard-coded in DynamoConstants class. If you want to keep table name also dynamic then change method signatures to include a tableName parameter and instead of getting table from DynamoManager class, use the method param. 

## Getting Started

### Required Prerequisites

To use this SDK you must have:

* A Java 8 development environment

### Get Started

Suppose you want to use AWS dynamo DB and perform different operation on it from your project then use this library and directly call the required functions with just partitionKeyValue, sortKeyValue and other key/value pair. You do not need to worry about understanding complete AWS library and how to implement it. Using this library is beneficial because to perform dynamo operation using AWS SDK requires multiple steps to be followed but this wrapper library implements AWS logic internally so all you have to do is call 1 method and get the response. 


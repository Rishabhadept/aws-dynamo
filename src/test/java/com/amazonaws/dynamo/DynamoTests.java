package com.amazonaws.dynamo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Helpful to test methods while development
 * 
 * @Ignore - because, Can not use it in actual build since it will actually create data on dynamo because there is no
 *         mocking
 * @author rishabhkasliwal
 *
 */
@Ignore
public class DynamoTests {

  @Test
  public void testCreateTable() {
    assertEquals(DynamoTableRequest.createTable("golo-test", "pKey", "sKey"), "golo-test");
  }

  @Test
  public void testUpdateMultipleAttributes() {
    Map<String, Object> dataMap = new HashMap<>();
    dataMap.put("name", "rk");
    dataMap.put("newkey", "newkeyValue");
    dataMap.put("year", 2000);
    assertNotNull(DynamoPutRequest.updateMultipleAttributes("mycollection", "id10", dataMap));
  }

  @Test
  public void testUpdateExistingAttributeConditionally() {
    assertNotNull(
        DynamoPutRequest.updateExistingAttributeConditionally("mycollection", "id1", "name", "rishabh", "year", 2000));

  }

  @Test
  public void testpostData2() {

    String vendorDocument = "{" + "    \"V01\": {" + "        \"Name\": \"horror Books\","
        + "        \"Offices\": [ \"Seattle\" ]" + "    }," + "    \"V02\": {"
        + "        \"Name\": \"New Publishers, Inc.\"," + "        \"Offices\": [ \"London\", \"New York\"" + "]" + "},"
        + "    \"V03\": {" + "        \"Name\": \"Better Buy Books\","
        + "\"Offices\": [ \"Tokyo\", \"Los Angeles\", \"Sydney\"" + "            ]" + "        }" + "    }";

    assertNotNull(DynamoPostRequest.postData("mycollection", "id2", "vendor", vendorDocument));
  }

  @Test
  public void testpostData() {

    String vendorDocument = "{" + "    \"V01\": {" + "        \"Name\": \"Acme Books\","
        + "        \"Offices\": [ \"Seattle\" ]" + "    }," + "    \"V02\": {"
        + "        \"Name\": \"New Publishers, Inc.\"," + "        \"Offices\": [ \"London\", \"New York\"" + "]" + "},"
        + "    \"V03\": {" + "        \"Name\": \"Better Buy Books\","
        + "\"Offices\": [ \"Tokyo\", \"Los Angeles\", \"Sydney\"" + "            ]" + "        }" + "    }";

    Map<String, Object> dataMap = new HashMap<>();
    dataMap.put("name", "rishabh");
    dataMap.put("year", 1995);
    dataMap.put("vendor", vendorDocument);

    assertNotNull(DynamoPostRequest.postData("mycollection", "id1", dataMap));

  }

  @Test
  public void testpostBulkData() {

    Map<String, Map<String, Object>> data = new HashMap<>();

    Map<String, Object> dataMap = new HashMap<>();
    dataMap.put("name", "kasliwal");
    dataMap.put("year", 2000);
    dataMap.put("vendor", false);

    Map<String, Object> dataMap2 = new HashMap<>();
    dataMap2.put("name", "jain");
    dataMap2.put("year", 1998);
    dataMap2.put("vendor", "N/A");

    data.put("id3", dataMap);
    data.put("id4", dataMap2);

    assertNotNull(DynamoPostRequest.postBulkData("mycollection", data));

  }

  @Test
  public void testdeleteItemConditional() {
    assertNotNull(DynamoDeleteRequest.deleteItemConditional("mycollection", "id3", "name", "kasliwal"));

  }

  @Test
  public void testdeleteItem() {
    assertNotNull(DynamoDeleteRequest.deleteItem("mycollection", "id4"));

  }

  @Test
  public void testScanItem() {
    assertNotNull(DynamoGetRequest.scanItem("name", "rishabh"));

  }

  @Test
  public void testRetrieveItem() {
    assertNotNull(DynamoGetRequest.retrieveItem("mycollection", "id2"));
  }

}

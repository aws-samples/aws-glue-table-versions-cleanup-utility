// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package software.aws.glue.tableversions.utils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

/**
 * This is a utility class with methods to write items to DynamoDB table. from /
 * to a DynamoDB table.
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class DDBUtil {

	public boolean insertCleanupStatusToDynamoDB(AmazonDynamoDB ddbClient, String ddbTableName, String hashKey,
			String rangeKey, long executionId, String executionBatchId, String databaseName, String tableName,
			int numTableVersionsB4Cleanup, int numVersionsRetained, int numDeletedVersions) {

		boolean itemInserted = false;
		DynamoDB dynamoDB = new DynamoDB(ddbClient);
		Table table = dynamoDB.getTable(ddbTableName);
		Item item = new Item()
				.withPrimaryKey(hashKey, executionId)
				.withNumber(rangeKey, Long.parseLong(executionBatchId))
				.withString("table_name", tableName)
				.withString("database_name", databaseName)
				.withNumber("number_of_versions_before_cleanup", numTableVersionsB4Cleanup)
				.withNumber("number_of_versions_retained", numVersionsRetained)
				.withNumber("number_of_versions_deleted", numDeletedVersions);
		// Write the item to the table
		PutItemOutcome outcome = table.putItem(item);
		int statusCode = outcome.getPutItemResult().getSdkHttpMetadata().getHttpStatusCode();
		if (statusCode == 200) {
			itemInserted = true;
			System.out.println("Table version inserted to DynamoDB table: " + ddbTableName);
		}
		return itemInserted;
	}

	/**
	 * Method to write Table version details to a DynamoDB table.
	 * 
	 * @param dynamoDBClient
	 * @param dynamoDBTblName
	 * @param primaryPartKey
	 * @param primarySortKey
	 * @param executionBatchId
	 * @param databaseName
	 * @param tableName
	 * @param notificationTime
	 * @return
	 */
	public boolean insertTableDetailsToDynamoDB(AmazonDynamoDB ddbClient, String ddbTableName, String hashKey,
			String rangeKey, long executionBatchId, String databaseName, String tableName, String messageSentTime) {

		boolean itemInserted = false;
		DynamoDB dynamoDB = new DynamoDB(ddbClient);
		Table table = dynamoDB.getTable(ddbTableName);
		Item item = new Item().withPrimaryKey(hashKey, executionBatchId)
				.withString(rangeKey, databaseName.concat("|").concat(tableName)).withString("table_name", tableName)
				.withString("database_name", databaseName).withString("message_sent_time", messageSentTime);
		// Write the item to the table
		PutItemOutcome outcome = table.putItem(item);
		int statusCode = outcome.getPutItemResult().getSdkHttpMetadata().getHttpStatusCode();
		if (statusCode == 200) {
			itemInserted = true;
			System.out.println("Table version inserted to DynamoDB table: " + ddbTableName);
		}
		return itemInserted;
	}

}
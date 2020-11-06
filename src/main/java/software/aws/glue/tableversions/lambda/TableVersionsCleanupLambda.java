// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package software.aws.glue.tableversions.lambda;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.TableVersion;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;

import software.aws.glue.tableversions.utils.DDBUtil;
import software.aws.glue.tableversions.utils.GlueTable;
import software.aws.glue.tableversions.utils.GlueUtil;
import software.aws.glue.tableversions.utils.TableVersionStatus;

/**
 * This class has AWS Lambda Handler method. Upon invocation, it gets an event
 * from source SQS queue, gets the message(s).
 * 
 * For each message, it takes the following actions: 1. Parse the message and
 * get Table name 2. Fetch list of table versions 3. Determine the list of table
 * versions to retains 4. Delete old table versions 5. Insert a record into
 * DynamoDB table with the statistics
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class TableVersionsCleanupLambda implements RequestHandler<SQSEvent, Object> {

	@Override
	public String handleRequest(SQSEvent event, Context context) {

		String region = Optional.ofNullable(System.getenv("region")).orElse("us-east-1");
		String ddbTableName = Optional.ofNullable(System.getenv("ddb_table_name"))
				.orElse("glue_table_version_cleanup_statistics");
		String hashKey = Optional.ofNullable(System.getenv("hash_key")).orElse("execution_id");
		String rangeKey = Optional.ofNullable(System.getenv("range_key")).orElse("execution_batch_id");
		int numberofVersionsToRetain = Ints
				.tryParse(Optional.ofNullable(System.getenv("number_of_versions_to_retain")).orElse("100"));

		System.out.println("Region: " + region);
		System.out.println("Number of table versions to retain: " + numberofVersionsToRetain);
		System.out.println("DynamoDB Table to track statistics: " + ddbTableName);

		AWSGlue glueClient = AWSGlueClientBuilder.standard().withRegion(region).build();
		AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();

		if (numberofVersionsToRetain < 50) {
			throw new RuntimeException();
		} else {
			System.out.println("Number of messages in SQS Event: " + event.getRecords().size());
			List<SQSMessage> sqsMessages = event.getRecords();
			processEvent(glueClient, dynamoDBClient, sqsMessages, numberofVersionsToRetain, ddbTableName, hashKey,
					rangeKey);
		}
		return "SNS event to Lambda processed successfully!";
	}

	/**
	 * This method processes SQS event
	 * 
	 * @param glueClient
	 * @param dynamoDBClient
	 * @param sqsMessages
	 * @param numberofVersionsToRetain
	 * @param dynamoDBTableName
	 * @param primaryPartKey
	 * @param primarySortKey
	 */
	public void processEvent(AWSGlue glueClient, AmazonDynamoDB dynamoDBClient, List<SQSMessage> sqsMessages,
			int numberofVersionsToRetain, String dynamoDBTableName, String hashKey, String rangeKey) {

		DDBUtil ddbUtil = new DDBUtil();
		GlueUtil glueUtil = new GlueUtil();
		List<TableVersionStatus> tblVersionsNotDeletedMasterList = new ArrayList<TableVersionStatus>();

		for (SQSMessage sqsMessage : sqsMessages) {
			long executionId = System.currentTimeMillis();
			// get Execution Batch Id from Message Attributes
			String executionBatchId = "";
			for (Entry<String, MessageAttribute> entry : sqsMessage.getMessageAttributes().entrySet()) {
				if ("ExecutionBatchId".equalsIgnoreCase(entry.getKey())) {
					executionBatchId = entry.getValue().getStringValue();
					System.out.println("Execution Batch Id: " + executionBatchId);
				}
			}

			// de-serialize SQS message to GlueTable
			Gson gson = new Gson();
			String message = new String(sqsMessage.getBody());
			GlueTable glueTable = gson.fromJson(message, GlueTable.class);
			System.out.printf("Process event for table '%s' under database '%s' \n", glueTable.getTableName(),
					glueTable.getDatabaseName());

			// get table versions
			List<TableVersion> tableVersionList = glueUtil.getTableVersions(glueClient, glueTable.getTableName(),
					glueTable.getDatabaseName());

			if (tableVersionList.size() > numberofVersionsToRetain) {
				// identify the versions that are older than numberofVersionsToRetain
				List<List<Integer>> lists = glueUtil.determineOldVersions(tableVersionList, glueTable.getTableName(),
						glueTable.getDatabaseName(), numberofVersionsToRetain);
				List<Integer> versionsToKeep = lists.get(0);
				List<Integer> versionsToDelete = lists.get(1);

				System.out.printf("For table '%s', versions to be deleted: %d, versions to be retaind: %d \n",
						glueTable.getTableName(), versionsToDelete.size(), versionsToKeep.size());

				// delete older versions
				List<TableVersionStatus> tblVersionsNotDeletedList = glueUtil.deleteTableVersions(glueClient,
						versionsToDelete, glueTable.getTableName(), glueTable.getDatabaseName());

				int numTableVersionsB4Cleanup = tableVersionList.size();
				int numDeletedVersions = versionsToDelete.size() - tblVersionsNotDeletedList.size();

				boolean itemInserted = ddbUtil.insertCleanupStatusToDynamoDB(dynamoDBClient, dynamoDBTableName, hashKey,
						rangeKey, executionId, executionBatchId, glueTable.getDatabaseName(), glueTable.getTableName(),
						numTableVersionsB4Cleanup, versionsToKeep.size(), numDeletedVersions);

				if (tblVersionsNotDeletedList.size() == 0)
					System.out.printf("Older versions of table '%s' under database '%s' were deleted. \n",
							glueTable.getTableName(), glueTable.getDatabaseName());
				else
					tblVersionsNotDeletedMasterList.addAll(tblVersionsNotDeletedList);
			} else {
				System.out.printf("Table '%s' does not have more than %d versions. Skipping. \n",
						glueTable.getTableName(), numberofVersionsToRetain);
			}
		}
	}
}
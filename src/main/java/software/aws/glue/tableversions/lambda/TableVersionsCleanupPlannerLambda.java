// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package software.aws.glue.tableversions.lambda;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;

import software.aws.glue.tableversions.utils.DDBUtil;
import software.aws.glue.tableversions.utils.GlueTable;
import software.aws.glue.tableversions.utils.GlueUtil;
import software.aws.glue.tableversions.utils.SQSUtil;

/**
 * This class has AWS Lambda Handler method. Upon invocation, it takes the
 * following actions: 1. it fetches all databases form Glue Catalog 2. for each
 * database, fetches all of its tables 3. for each table, it publishes table
 * and database names to SQS queue.
 * 
 * @author Ravi Itha, Amazon Web Services, Inc.
 *
 */
public class TableVersionsCleanupPlannerLambda implements RequestHandler<Object, String> {

	@Override
	public String handleRequest(Object input, Context context) {

		String separator = Optional.ofNullable(System.getenv("separator")).orElse("$");
		String region = Optional.ofNullable(System.getenv("region")).orElse(Regions.US_EAST_1.getName());
		String databaseNamesStringLiteral = Optional.ofNullable(System.getenv("database_names_string_literal"))
				.orElse("database_1$database_2");
		String sqsQueueURI = Optional.ofNullable(System.getenv("sqs_queue_url"))
				.orElse("https://sqs.us-east-1.amazonaws.com/1234567890/table_versions_cleanup_planner_queue.fifo");
		String ddbTableName = Optional.ofNullable(System.getenv("ddb_table_name"))
				.orElse("glue_table_version_cleanup_planner");
		String hashKey = Optional.ofNullable(System.getenv("hash_key")).orElse("execution_batch_id");
		String rangeKey = Optional.ofNullable(System.getenv("range_key")).orElse("database_name_table_name");

		long executionBatchId = System.currentTimeMillis();

		AWSSecurityTokenService client = AWSSecurityTokenServiceClientBuilder.standard().build();
		GetCallerIdentityRequest request = new GetCallerIdentityRequest();
		GetCallerIdentityResult response = client.getCallerIdentity(request);
		String homeCatalogId = response.getAccount();
		context.getLogger().log("Catalog Id: " + homeCatalogId);

		context.getLogger().log("Input: " + input);
		printEnvVariables(sqsQueueURI, databaseNamesStringLiteral, separator, region, ddbTableName, hashKey, rangeKey);

		// Create objects for AWS Glue and Amazon SQS
		AWSGlue glue = AWSGlueClientBuilder.standard().withRegion(region).build();
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(region).build();
		AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();

		DDBUtil ddbUtil = new DDBUtil();
		SQSUtil sqsUtil = new SQSUtil();
		GlueUtil glueUtil = new GlueUtil();
		List<String> databaseNames = new ArrayList<String>();
		List<Database> databaseList = new ArrayList<Database>();
		AtomicInteger numberOfTablesExported = new AtomicInteger();

		// When list of databases are provided as a token separated values then the
		// cleanup process will be initiated for those databases.
		// else, it imports the cleanup process will be initiated for all databases

		if (databaseNamesStringLiteral.equalsIgnoreCase("")) {
			databaseList = glueUtil.getDatabases(glue, homeCatalogId);
		} else {
			databaseNames = tokenizeStrings(databaseNamesStringLiteral, separator);
			for (String databaseName : databaseNames) {
				Database database = glueUtil.getDatabase(glue, homeCatalogId, databaseName);
				if (Optional.ofNullable(database).isPresent())
					databaseList.add(database);
			}
		}

		List<Table> tableList = glueUtil.getTables(glue, databaseList, homeCatalogId);
		for (Table table : tableList) {
			GlueTable tableMessage = new GlueTable();
			tableMessage.setDatabaseName(table.getDatabaseName());
			tableMessage.setTableName(table.getName());

			Gson gson = new Gson();
			String message = gson.toJson(tableMessage);

			// Write a message to Amazon SQS queue.
			boolean messageSentToSQS = sqsUtil.sendTableSchemaToSQSQueue(sqs, sqsQueueURI, message, executionBatchId, table.getDatabaseName());
			if (messageSentToSQS) {
				String messageSentTime = new Date().toString();
				numberOfTablesExported.incrementAndGet();
				ddbUtil.insertTableDetailsToDynamoDB(dynamoDBClient, ddbTableName, hashKey, rangeKey, executionBatchId,
						table.getDatabaseName(), table.getName(), messageSentTime);
			}
		}
		System.out.printf("Number of messages written to SQS Queue: %d \n", numberOfTablesExported.get());
		return "TableVersionsCleanupPlannerLambda completed successfully!";
	}

	/**
	 * This method prints environment variables
	 * 
	 * @param sourceGlueCatalogId
	 * @param topicArn
	 * @param ddbTblNameForDBStatusTracking
	 */
	public static void printEnvVariables(String sqsQueueURI, String databaseNamesStringLiteral, String separator,
			String region, String ddbTableName, String hashKey, String rangeKey) {
		System.out.println("Region: " + region);
		System.out.println("SQS URL: " + sqsQueueURI);
		System.out.println("Separator: " + separator);
		System.out.println("Database names string literal: " + sqsQueueURI);
		System.out.println("DynamoDB table Name: " + ddbTableName);
		System.out.println("DynamoDB table - hash key: " + hashKey);
		System.out.println("DynamoDB table - range key: " + rangeKey);
	}

	/**
	 * This method tokenizes strings using a provided separator
	 * 
	 * @param str
	 * @param separator
	 * @return
	 */
	public static List<String> tokenizeStrings(String str, String separator) {
		List<String> tokenList = Collections.list(new StringTokenizer(str, separator)).stream()
				.map(token -> (String) token).collect(Collectors.toList());
		return tokenList;
	}
}
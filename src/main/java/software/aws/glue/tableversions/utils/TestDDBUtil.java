package software.aws.glue.tableversions.utils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

public class TestDDBUtil {

	public static void main(String[] args) {

		String ddbTableName_1 = "glue_table_version_cleanup_planner";
		String ddbTableName_2 = "glue_table_version_cleanup_statistics";
		long executionId = System.currentTimeMillis();

		DDBUtil ddbUtil = new DDBUtil();

		String hashKey_1 = "execution_batch_id";
		String rangeKey_1 = "database_name_table_name";

		String hashKey_2 = "execution_id";
		String rangeKey_2 = "execution_batch_id";

		String databaseName = "test_db";
		String tableName = "test_table";
		int numTableVersionsB4Cleanup = 20;
		int numVersionsRetained = 10;
		int numDeletedVersions = 10;

		long executionBatchId = System.currentTimeMillis();
		AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard().withRegion("us-east-1").build();
		String notificationTime = new java.util.Date().toString();

		ddbUtil.insertTableDetailsToDynamoDB(ddbClient, ddbTableName_1, hashKey_1, rangeKey_1, executionBatchId,
				databaseName, tableName, notificationTime);

		ddbUtil.insertCleanupStatusToDynamoDB(ddbClient, ddbTableName_2, hashKey_2, rangeKey_2, executionId,
				Long.toString(executionBatchId), databaseName, tableName, numTableVersionsB4Cleanup,
				numVersionsRetained, numDeletedVersions);
	}

}

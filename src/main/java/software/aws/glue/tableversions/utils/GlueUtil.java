// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package software.aws.glue.tableversions.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableVersionResult;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableVersionsRequest;
import com.amazonaws.services.glue.model.GetTableVersionsResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableVersion;
import com.amazonaws.services.glue.model.TableVersionError;
import com.google.common.collect.Lists;

public class GlueUtil {

	/**
	 * Get all versions of a table
	 * 
	 * @param glueClient
	 * @param tableName
	 * @param databaseName
	 * @return
	 */
	public List<TableVersion> getTableVersions(AWSGlue glueClient, String tableName, String databaseName) {

		List<TableVersion> masterTableVersionList = new ArrayList<TableVersion>();

		// Prepare GetTableVersionsRequest and Get Table versions
		GetTableVersionsRequest getTableVersionsRequest = new GetTableVersionsRequest();
		getTableVersionsRequest.setTableName(tableName);
		getTableVersionsRequest.setDatabaseName(databaseName);
		GetTableVersionsResult getTableVersionsResult = glueClient.getTableVersions(getTableVersionsRequest);
		List<TableVersion> tableVersionList = getTableVersionsResult.getTableVersions();
		masterTableVersionList.addAll(tableVersionList);

		// Get and check next token if it is null
		String tableVersionsNextToken = getTableVersionsResult.getNextToken();
		if (Optional.ofNullable(tableVersionsNextToken).isPresent()) {
			do {
				getTableVersionsRequest = new GetTableVersionsRequest();
				getTableVersionsRequest.setTableName(tableName);
				getTableVersionsRequest.setDatabaseName(databaseName);
				getTableVersionsRequest.setNextToken(tableVersionsNextToken);
				getTableVersionsResult = glueClient.getTableVersions(getTableVersionsRequest);
				tableVersionList = getTableVersionsResult.getTableVersions();
				masterTableVersionList.addAll(tableVersionList);
				tableVersionsNextToken = getTableVersionsResult.getNextToken();
			} while (Optional.ofNullable(tableVersionsNextToken).isPresent());
		}
		return masterTableVersionList;
	}

	/**
	 * Get all tables of a database
	 * 
	 * @param glue
	 * @param masterdatabaseList
	 * @return
	 */
	public List<Table> getTables(AWSGlue glue, List<Database> databaseList, String homeCatalogId) {

		List<Table> masterTableList = new ArrayList<Table>();
		// Iterate through all the databases
		for (Database db : databaseList) {
			String databaseName = db.getName();
			// Get tables
			GetTablesRequest getTablesRequest = new GetTablesRequest();
			getTablesRequest.setDatabaseName(databaseName);
			GetTablesResult getTablesResult = glue.getTables(getTablesRequest);
			List<Table> tableList = getTablesResult.getTableList();
			for (Table table : tableList) {
				if (!Optional.ofNullable(table.getTargetTable()).isPresent()) {
					masterTableList.add(table);
				} else {
					System.out.printf("Table '%s' under database '%s' seems to have resource linked from AWS Account Id: '%s'. So, it will be skipped. \n",
							table.getName(), table.getDatabaseName(), table.getTargetTable().getCatalogId());
				}
			}
			String tableResultNextToken = getTablesResult.getNextToken();
			if (Optional.ofNullable(tableResultNextToken).isPresent()) {
				do {
					getTablesRequest = new GetTablesRequest();
					getTablesRequest.setDatabaseName(databaseName);
					getTablesRequest.setNextToken(tableResultNextToken);
					getTablesResult = glue.getTables(getTablesRequest);
					tableList = getTablesResult.getTableList();
					for (Table table : tableList) {
						if (!Optional.ofNullable(table.getTargetTable()).isPresent()) {
							masterTableList.add(table);
						} else {
							System.out.printf("Table '%s' under database '%s' seems to have resource linked from AWS Account Id: '%s'. So, it will be skipped. \n",
									table.getName(), table.getDatabaseName(), table.getTargetTable().getCatalogId());
						}
					}
					tableResultNextToken = getTablesResult.getNextToken();
				} while (Optional.ofNullable(tableResultNextToken).isPresent());
			}
		}
		return masterTableList;
	}

	/**
	 * This method gets AWS Glue Database based on a provided name
	 * 
	 * @param glue
	 * @param homeCatalogId
	 * @return
	 */
	public Database getDatabase(AWSGlue glue, String homeCatalogId, String databaseName) {
		Database database = null;
		GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest();
		getDatabaseRequest.setName(databaseName);
		getDatabaseRequest.setCatalogId(homeCatalogId);
		try {
			GetDatabaseResult getDatabaseResult = glue.getDatabase(getDatabaseRequest);
			database = getDatabaseResult.getDatabase();
		} catch (EntityNotFoundException exception) {
			System.out.printf(
					"There is no database exist with name '%s' in AWS Account %s. It may be possible it is a resource linked from other database. "
							+ "Hence, it will be skipped from clean-up process. \n",
					databaseName, homeCatalogId);
		}
		return database;
	}

	/**
	 * Method to get all databases
	 * 
	 * @param glue
	 * @return
	 */
	public List<Database> getDatabases(AWSGlue glue, String homeCatalogId) {
		List<Database> masterDatabaseList = new ArrayList<Database>();
		GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest();
		GetDatabasesResult getDatabasesResult = glue.getDatabases(getDatabasesRequest);
		List<Database> databaseList = getDatabasesResult.getDatabaseList();
		
		// filter databases that are resource linked
		for (Database database : databaseList) {
			if (!Optional.ofNullable(database.getTargetDatabase()).isPresent()) {
				masterDatabaseList.add(database);
			} else {
				System.out.printf("Database '%s' seems to have resource linked from AWS Account Id: '%s'. So, it will be skipped. \n",
						database.getName(), database.getTargetDatabase().getCatalogId());
			}
		}
		String databaseResultNextToken = getDatabasesResult.getNextToken();
		if (Optional.ofNullable(databaseResultNextToken).isPresent()) {
			do {
				// creating a new GetDatabasesRequest using next token.
				getDatabasesRequest = new GetDatabasesRequest();
				getDatabasesRequest.setNextToken(databaseResultNextToken);
				getDatabasesResult = glue.getDatabases(getDatabasesRequest);
				databaseList = getDatabasesResult.getDatabaseList();
				// filter databases that are resource linked
				for (Database database : databaseList) {
					if (!Optional.ofNullable(database.getTargetDatabase()).isPresent()) {
						masterDatabaseList.add(database);
					} else {
						System.out.printf("Database '%s' seems to have resource linked from AWS Account Id: '%s'. So, it will be skipped. \n",
								database.getName(), database.getTargetDatabase().getCatalogId());
					}
				}
				databaseResultNextToken = getDatabasesResult.getNextToken();
			} while (Optional.ofNullable(databaseResultNextToken).isPresent());
		}
		return masterDatabaseList;
	}

	/**
	 * Method to delete a list of tables versions
	 * 
	 * @param glueClient
	 * @param listofVersionsToDelete
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public List<TableVersionStatus> deleteTableVersions(AWSGlue glueClient, List<Integer> listofVersionsToDelete,
			String tableName, String databaseName) {

		// This code deletes 100 versions at a time. So, it breaks the input list into
		// smaller
		// Lists of size 100.
		List<TableVersionStatus> versionsNotDeleted = new ArrayList<TableVersionStatus>();
		List<List<Integer>> listofSmallerLists = Lists.partition(listofVersionsToDelete, 100);

		for (List<Integer> smallerList : listofSmallerLists) {
			List<String> versionIdSmallerList = new ArrayList<String>();
			for (Integer versionId : smallerList) {
				versionIdSmallerList.add(Integer.toString(versionId));
			}
			// Batch Delete Table versions 100 items at a time.
			if (versionIdSmallerList.size() >= 1) {

				// BatchDeleteTableVersionRequest
				BatchDeleteTableVersionRequest batchDeleteTableVersionRequest = new BatchDeleteTableVersionRequest();
				batchDeleteTableVersionRequest.setDatabaseName(databaseName);
				batchDeleteTableVersionRequest.setTableName(tableName);
				batchDeleteTableVersionRequest.setVersionIds(versionIdSmallerList);

				// execute batchDelete operation
				BatchDeleteTableVersionResult batchDeleteTableVersionResult = glueClient
						.batchDeleteTableVersion(batchDeleteTableVersionRequest);

				// Check the result and re-process rejected records
				batchDeleteTableVersionResult.getSdkHttpMetadata().getHttpStatusCode();
				if (batchDeleteTableVersionResult.getErrors().isEmpty()) {
					System.out.printf(
							"Up to 100 table versions deleted successfully for table '%s' under database '%s' \n",
							tableName, databaseName);
				} else {
					List<TableVersionError> tableVersionErrors = batchDeleteTableVersionResult.getErrors();
					for (TableVersionError tvError : tableVersionErrors) {
						TableVersionStatus tvStatus = new TableVersionStatus();
						tvStatus.setDatabaseName(databaseName);
						tvStatus.setDeleted(false);
						tvStatus.setTableName(tvError.getTableName());
						tvStatus.setVersionId(tvError.getVersionId());
						versionsNotDeleted.add(tvStatus);
					}
				}
			}
			versionIdSmallerList.clear();
		}
		return versionsNotDeleted;
	}

	/**
	 * Method to determine how many table versions to kept and how many to delete.
	 * 
	 * @param tableVersionList
	 * @param databaseName
	 * @param tableName
	 * @param numberofVersionsToKeep
	 * @return
	 */
	public List<List<Integer>> determineOldVersions(List<TableVersion> tableVersionList, String tableName,
			String databaseName, int numberofVersionsToKeep) {

		List<Integer> versionIdList = new ArrayList<Integer>();
		for (TableVersion tableVersion : tableVersionList) {
			// System.out.printf("Table name: %s, Table version: %s \n", tableName,
			// tableVersion.getVersionId());
			versionIdList.add(Integer.parseInt(tableVersion.getVersionId()));
		}
		// sort the versions in descending order
		Collections.sort(versionIdList, Collections.reverseOrder());
		System.out.printf("%d table versions found for table: %s \n", versionIdList.size(), tableName);
		System.out.printf("%d is the current (latest) version of the table: %s \n", versionIdList.get(0), tableName);

		// Break the list into two parts. The first part is all the most recent table
		// versions that need to be retained.
		// the second list contains all the older table version which need to be deleted
		List<List<Integer>> lists = new ArrayList<List<Integer>>(versionIdList.stream()
				.collect(Collectors.partitioningBy(s -> versionIdList.indexOf(s) > numberofVersionsToKeep - 1))
				.values());
		return lists;
	}

}

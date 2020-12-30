# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import os
from src.utils.glue_util import get_glue_client, get_all_table_versions, determine_old_versions, delete_table_versions
from src.utils.helper import get_current_time_in_millis
from src.utils.ddb_util import get_dynamodb_client, insert_to_cleanup_dynamodb


def process(event):
    """
    For each table, fetches the table versions,
    identifies versions that are older than given number of versions to retain,
    deletes the older versions and 
    writes the stats to DynamoDB

    :param event:
    :return:
    """
    try:
        # Clients
        glue_client = get_glue_client()
        ddb_client = get_dynamodb_client()

        # Environment Variables
        number_of_versions_to_retain = int(os.environ['number_of_versions_to_retain']) or 100
        ddb_table_name = os.environ['ddb_table_name'] or 'glue_table_version_cleanup_statistics'
        hash_key = os.environ['hash_key'] or 'execution_id'
        range_key = os.environ['range_key'] or 'execution_batch_id'

        versions_not_deleted_all_tables = []
        number_of_versions_to_retain_min = 50

        if number_of_versions_to_retain < number_of_versions_to_retain_min:
            raise Exception(
                'Number of versions to be retained should be more than {}'.format(number_of_versions_to_retain_min))
        else:
            print('Number of records in SQS message: {}'.format(len(event['Records'])))
            for record in event['Records']:
                execution_id = get_current_time_in_millis()
                execution_batch_id = ''
                message_attributes = record['messageAttributes']
                for key, value in message_attributes.items():
                    if key.lower() == 'ExecutionBatchId'.lower():
                        execution_batch_id = value['stringValue']
                message = json.loads(record['body'])

                # Get table versions
                database_name = message['DatabaseName']
                table_name = message['TableName']
                table_version_list = get_all_table_versions(glue_client, database_name, table_name)

                if len(table_version_list) > number_of_versions_to_retain:
                    # Identify the versions that are older than number_of_versions_to_retain
                    list_of_lists = determine_old_versions(table_version_list, table_name, database_name,
                                                           number_of_versions_to_retain)
                    versions_to_retain = list_of_lists[0]
                    versions_to_delete = list_of_lists[1]
                    print('For table: {}, versions to be deleted: {}, versions to be retained: {}'.
                          format(table_name, len(versions_to_delete), len(versions_to_retain)))
                    # Delete older versions with chunks of 100 at a time
                    versions_not_deleted = delete_table_versions(glue_client, versions_to_delete, database_name,
                                                                 table_name)
                    if versions_not_deleted:
                        versions_not_deleted_all_tables.append(versions_not_deleted)
                    num_of_versions_before_cleanup = len(table_version_list)
                    num_of_versions_deleted = len(versions_to_delete) - len(versions_not_deleted)

                    # Insert to DynamoDB
                    insert_to_cleanup_dynamodb(ddb_client, ddb_table_name, hash_key, range_key,
                                               execution_id, execution_batch_id, database_name, table_name,
                                               num_of_versions_before_cleanup, number_of_versions_to_retain,
                                               num_of_versions_deleted)
                    print('Stats for table: {} written to DynamoDB successfully'.format(table_name))
                else:
                    print('Table {} does not have more than {} versions. Skipping.'.format(table_name,
                                                                                           number_of_versions_to_retain))

    except Exception as e:
        print('Cleanup Lambda execution failed')
        raise Exception(e)

    return 'Cleanup Lambda execution succeeded!'


def handler(event, context):
    """
    Entry point for the Lambda execution

    :param event:
    :param context:
    :return:
    """
    return process(event)

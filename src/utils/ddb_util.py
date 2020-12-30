# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os


def get_dynamodb_client():
    """
    DynamoDB Client
    
    :return:
    """
    return boto3.client('dynamodb')


def insert_to_planner_dynamodb(client, ddb_table_name, hash_key, execution_batch_id, range_key,
                               database_name, table_name, message_sent_time):
    """
    Inserts to Planner DynamoDB table

    :param client:
    :param ddb_table_name:
    :param hash_key:
    :param execution_batch_id:
    :param range_key:
    :param database_name:
    :param table_name:
    :param message_sent_time:
    :return:
    """
    client.put_item(
        TableName=ddb_table_name,
        Item={
            hash_key: {'N': str(execution_batch_id)},
            range_key: {'S': database_name + '|' + table_name},
            'table_name': {'S': table_name},
            'database_name': {'S': database_name},
            'message_sent_time': {'S': message_sent_time}
        })


def insert_to_cleanup_dynamodb(client, ddb_table_name, hash_key, range_key,
                               execution_id, execution_batch_id, database_name, table_name,
                               num_of_versions_before_cleanup, num_of_versions_retained, num_of_versions_deleted):
    """
    Inserts to Cleanup DynamoDB table

    :param client:
    :param ddb_table_name:
    :param hash_key:
    :param range_key:
    :param execution_id:
    :param execution_batch_id:
    :param database_name:
    :param table_name:
    :param num_of_versions_before_cleanup:
    :param num_of_versions_retained:
    :param num_of_versions_deleted:
    :return:
    """
    client.put_item(
        TableName=ddb_table_name,
        Item={
            hash_key: {'N': str(execution_id)},
            range_key: {'N': str(execution_batch_id)},
            'table_name': {'S': table_name},
            'database_name': {'S': database_name},
            'number_of_versions_before_cleanup': {'N': str(num_of_versions_before_cleanup)},
            'number_of_versions_retained': {'N': str(num_of_versions_retained)},
            'number_of_versions_deleted': {'N': str(num_of_versions_deleted)}
        })

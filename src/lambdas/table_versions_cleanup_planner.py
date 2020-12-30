# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import os
from src.utils.helper import split_str, get_current_time_in_millis, get_current_date_time
from src.utils.glue_util import get_glue_client, get_all_databases, get_all_tables
from src.utils.sqs_util import get_sqs_client, send_message
from src.utils.ddb_util import get_dynamodb_client, insert_to_planner_dynamodb


def prepare_sqs_message(database_name, table_name):
    """
    Generates the message to be sent to SQS

    :param database_name:
    :param table_name:
    :return:
    """
    message = {
        'DatabaseName': database_name,
        'TableName': table_name
    }
    return json.dumps(message)


def process():
    """
    Fetches the list of tables,
    pushes to SQS and DynamoDB

    :return:
    """
    try:
        # Clients
        glue_client = get_glue_client()
        sqs_client = get_sqs_client()
        ddb_client = get_dynamodb_client()

        # Environment Variables
        database_names_str_literal = ''
        separator = ''
        # Optional, value is empty if not provided
        if 'database_names_string_literal' in os.environ:
            database_names_str_literal = os.environ['database_names_string_literal']
        # Optional, value is empty if not provided
        if 'separator' in os.environ:
            separator = os.environ['separator'] or '$'
        sqs_queue_url = os.environ['sqs_queue_url']
        ddb_table_name = os.environ['ddb_table_name'] or 'glue_table_version_cleanup_planner'
        hash_key = os.environ['hash_key'] or 'execution_batch_id'
        range_key = os.environ['range_key'] or 'database_name_table_name'

        message_counter = 0
        database_list = []
        table_list = []
        execution_batch_id = get_current_time_in_millis()

        # Get databases
        if database_names_str_literal == '':
            database_list = get_all_databases(glue_client)
        else:
            database_list = split_str(database_names_str_literal, separator)
        # Get tables
        for database_name in database_list:
            table_list = get_all_tables(glue_client, database_name)
            for table_name in table_list:
                # Send to SQS
                message = prepare_sqs_message(database_name, table_name)
                message_sent = send_message(sqs_client, sqs_queue_url, message, 'ExecutionBatchId', execution_batch_id,
                                            database_name)
                if message_sent:
                    # Insert to DynamoDB for audit purpose
                    message_sent_time = get_current_date_time()
                    message_counter += 1
                    insert_to_planner_dynamodb(ddb_client, ddb_table_name, hash_key, execution_batch_id, range_key,
                                               database_name, table_name, message_sent_time)

        print('Table list is written to SQS queue and DynamoDB successfully')
        print('Number of messages written to SQS queue: {}'.format(message_counter))

    except Exception as e:
        print('Planner Lambda execution failed')
        raise Exception(e)

    return 'Planner Lambda execution succeeded!'


def handler(event, context):
    """
    Entry point for the Lambda execution

    :param event:
    :param context:
    :return:
    """
    return process()

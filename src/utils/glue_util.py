# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
from src.utils.helper import partition_list


def get_glue_client():
    """
    Glue Client

    :return:
    """
    return boto3.client('glue')


def get_all_databases(client):
    """
    Get all Glue databases

    :param client:
    :return database_list:
    """
    database_list = []
    paginator = client.get_paginator('get_databases')
    response_iterator = paginator.paginate()
    for response in response_iterator:
        for database_dict in response['DatabaseList']:
            # Check if the database is resource linked to another account
            if 'TargetDatabase' in database_dict:
                print(
                    'Database {} seems to have resource linked from AWS Account Id: {}. So, it will be skipped.'.format(
                        database_dict['Name'], database_dict['TargetDatabase']['CatalogId']))
            else:
                database_list.append(database_dict['Name'])
    return database_list


def get_all_tables(client, database_name):
    """
    Get all Glue tables

    :param client:
    :param database_name:
    :return table_list:
    """
    table_list = []
    paginator = client.get_paginator('get_tables')
    response_iterator = paginator.paginate(
        DatabaseName=database_name
    )
    for response in response_iterator:
        for table_dict in response['TableList']:
            table_list.append(table_dict['Name'])
    return table_list


def get_all_table_versions(client, database_name, table_name):
    """
    Get all Glue table versions

    :param client:
    :param database_name:
    :param table_name:
    :return table_version_list:
    """
    table_version_list = []
    paginator = client.get_paginator('get_table_versions')
    response_iterator = paginator.paginate(
        DatabaseName=database_name,
        TableName=table_name
    )
    for response in response_iterator:
        for table_dict in response['TableVersions']:
            table_version_list.append(table_dict)
    return table_version_list


def determine_old_versions(table_version_list, table_name, database_name, number_of_versions_to_retain):
    """
    Determine the versions that need to be retained and 
    versions that need to be deleted

    :param table_version_list:
    :param table_name:
    :param database_name:
    :param number_of_versions_to_retain:
    :return list_of_lists:
    """
    version_id_list = []
    list_of_lists = []
    for table_version in table_version_list:
        version_id_list.append(int(table_version['VersionId']))
    # Sort the versions in descending order
    version_id_list.sort(reverse=True)
    print('{} table versions found for table: {}'.format(len(version_id_list), table_name))
    print('{} is the current (latest) version of the table: {}'.format(version_id_list[0], table_name))
    list_of_lists.append(version_id_list[:number_of_versions_to_retain])
    list_of_lists.append(version_id_list[number_of_versions_to_retain:])
    return list_of_lists


def delete_table_versions_batch(client, database_name, table_name, versions):
    """
    Delete Glue table versions batch

    :param client:
    :param database_name:
    :param table_name:
    :param versions:
    :return response:
    """
    response = client.batch_delete_table_version(
        DatabaseName=database_name,
        TableName=table_name,
        VersionIds=versions
    )
    return response


def delete_table_versions(client, versions, database_name, table_name):
    """
    Partitions the input version list into chunks of 100 and 
    calls the delete batch method

    :param client:
    :param versions:
    :param database_name:
    :param table_name:
    :return versions_not_deleted:
    """
    versions_not_deleted = []
    versions_str_list = [str(x) for x in versions]
    chunks = list(partition_list(versions_str_list, 100))
    for chunk in chunks:
        response = delete_table_versions_batch(client, database_name, table_name, chunk)
        if not response['Errors']:
            print('Up to 100 table versions deleted successfully for table {} under database {}'.format(table_name,
                                                                                                        database_name))
        else:
            for error in response['Errors']:
                versions_not_deleted.append(error)
    return versions_not_deleted

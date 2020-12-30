# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os


def get_sqs_client():
    """
    SQS Client

    :return:
        """
    return boto3.client('sqs')


def send_message(client, queue_url, message, message_attribute_key, message_attribute_value, group_id):
    """
    Sends message to SQS

    :param client:
    :param queue_url:
    :param message:
    :param message_attribute_key:
    :param message_attribute_value:
    :param group_id:
    :return message_sent:
    """
    message_sent = False
    response = client.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
        MessageAttributes={
            message_attribute_key: {
                'StringValue': str(message_attribute_value),
                'DataType': 'String'
            }
        },
        MessageGroupId=group_id
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        message_sent = True
    return message_sent

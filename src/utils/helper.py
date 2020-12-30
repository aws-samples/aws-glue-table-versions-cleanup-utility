# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import time
from datetime import datetime


def split_str(input_str, separator):
    """
    Splits the string based on separator

    :param input_str:
    :param separator:
    :return:
    """
    return input_str.split(separator)


def get_current_time_in_millis():
    """
    Gets current time in millis

    :return:
    """
    return round(time.time() * 1000)


def get_current_date_time():
    """
    Gets current date time

    :return:
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def partition_list(l, n):
    """
    Partitions the given list

    :param l:
    :param n:
    :return:
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]

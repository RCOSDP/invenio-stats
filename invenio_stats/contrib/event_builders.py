# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Signal receivers for certain events."""

from __future__ import absolute_import, print_function

import datetime
import hashlib
import uuid

from flask import request

from ..utils import get_user


def celery_task_event_builder(event, sender_app, exec_data=None, user_data=None, **kwargs):
    """Build a celery-task event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),

        # What:
        task_id=exec_data['task_id'],
        task_name=exec_data['task_name'],
        task_state=exec_data['task_state'],
        start_time=exec_data['start_time'],
        end_time=exec_data['end_time'],
        total_records=exec_data['total_records'],
        repository_name=exec_data['repository_name'],
        execution_time=exec_data['execution_time'],

        # Who:
        # **get_user()
        # Must retrieve the user data from caller
        # Task has no access to request
        ip_address=user_data['ip_address'],
        user_agent=user_data['user_agent'],
        user_id=user_data['user_id'],
        session_id=user_data['session_id']
    ))
    return event


def file_download_event_builder(event, sender_app, obj=None, **kwargs):
    """Build a file-download event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        bucket_id=str(obj.bucket_id),
        file_id=str(obj.file_id),
        file_key=obj.key,
        size=obj.file.size,
        referrer=request.referrer,
        accessrole=obj.file.json['accessrole'],
        userrole=obj.userrole,
        site_license_flag=obj.site_license_flag,
        index_list=obj.index_list,
        cur_user_id=obj.userid,
        # Who:
        **get_user()
    ))
    return event


def file_preview_event_builder(event, sender_app, obj=None, **kwargs):
    """Build a file-preview event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        bucket_id=str(obj.bucket_id),
        file_id=str(obj.file_id),
        file_key=obj.key,
        size=obj.file.size,
        referrer=request.referrer,
        accessrole=obj.file.json['accessrole'],
        userrole=obj.userrole,
        site_license_flag=obj.site_license_flag,
        index_list=obj.index_list,
        cur_user_id=obj.userid,
        # Who:
        **get_user()
    ))
    return event


def build_celery_task_unique_id(doc):
    """Build celery task unique identifier."""
    key = '{0}_{1}_{2}'.format(
        doc['task_id'], doc['task_name'], doc['repository_name']
    )
    doc['unique_id'] = str(uuid.uuid3(uuid.NAMESPACE_DNS, key))
    return doc


def build_file_unique_id(doc):
    """Build file unique identifier."""
    key = '{0}_{1}_{2}_{3}_{4}_{5}_{6}'.format(
        doc['bucket_id'], doc['file_id'], doc['userrole'], doc['accessrole'],
        doc['index_list'], doc['site_license_flag'], doc['country'],
        doc['cur_user_id']
    )
    doc['unique_id'] = str(uuid.uuid3(uuid.NAMESPACE_DNS, key))
    return doc


def build_record_unique_id(doc):
    """Build record unique identifier."""
    record_index_names = copy_record_index_list(doc)
    doc['unique_id'] = '{0}_{1}_{2}_{3}'.format(
        doc['record_id'], doc['country'], doc['cur_user_id'],
        record_index_names)
    doc['hostname'] = '{}'.format(resolve_address(doc['remote_addr']))
    return doc


def copy_record_index_list(doc, aggregation_data=None):
    """Copy record index list."""
    record_index_names = ''
    list = doc['record_index_list']
    if list:
        agg_record_index_list = []
        for index in list:
            agg_record_index_list.append(index['index_name'])
            record_index_names = ", ".join(agg_record_index_list)
    return record_index_names


def record_view_event_builder(event, sender_app, pid=None, record=None,
                              **kwargs):
    """Build a record-view event."""
    # get index information
    index_list = []
    if record.get('navi') is not None:
        for index in record.get('navi'):
            index_list.append(dict(
                index_id=index[1],
                index_name=index[3],
                index_name_en=index[4]
            ))
    cur_user = get_user()
    cur_user_id = cur_user['user_id'] if cur_user['user_id'] else 'guest'

    record_name = record.get('item_title', '') if record is not None else ''

    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        record_id=str(record.id),
        record_name=record_name,
        record_index_list=index_list,
        pid_type=pid.pid_type,
        pid_value=str(pid.pid_value),
        referrer=request.referrer,
        cur_user_id=cur_user_id,
        remote_addr=request.remote_addr,
        # Who:
        **get_user()
    ))
    return event


def top_view_event_builder(event, sender_app, **kwargs):
    """Build a top-view event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        referrer=request.referrer,
        remote_addr=request.remote_addr,
        # Who:
        **get_user()
    ))
    return event


def build_top_unique_id(doc):
    """Build top unique identifier."""
    doc['unique_id'] = '{0}_{1}'.format("top", "view")
    return doc


def build_item_create_unique_id(doc):
    """Build item_create unique identifier."""
    doc['unique_id'] = '{0}_{1}_{2}'.format("item", "create", doc['pid_value'])
    doc['hostname'] = '{}'.format(resolve_address(doc['remote_addr']))
    return doc


def resolve_address(addr):
    """Resolve the ip address string addr and return its DNS name. If no name is found, return None."""
    from socket import gethostbyaddr, herror
    try:
        record = gethostbyaddr(addr)

    except herror as exc:
        print('an error occurred while resolving ', addr, ': ', exc)
        return None

    return record[0]


def search_event_builder(event, sender_app, search_args=None, **kwargs):
    """Build a search event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        referrer=request.referrer,
        search_detail=search_args.to_dict(flat=False),
        # Who:
        **get_user()
    ))
    return event


def build_search_unique_id(doc):
    """Build search unique identifier."""
    doc['unique_id'] = '{0}_{1}'.format(
        doc['search_detail']['search_key'],
        doc['search_detail']['search_type'])
    return doc


def build_search_detail_condition(doc):
    """Build search detail condition."""
    search_detail = {}
    for key, value in doc['search_detail'].items():
        str_val = ' '.join(value)
        if key == 'q':
            search_detail['search_key'] = str_val
        elif len(value) > 0:
            search_detail[key] = str_val

    doc['search_detail'] = search_detail
    return doc


def item_create_event_builder(event, sender_app, item_id=None, **kwargs):
    """Build a item-create event."""
    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        referrer=request.referrer,
        remote_addr=request.remote_addr,
        pid_type=item_id.pid_type,
        pid_value=str(item_id.pid_value),
        # Who:
        **get_user()
    ))
    return event

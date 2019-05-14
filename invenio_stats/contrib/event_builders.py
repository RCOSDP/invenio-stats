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
        userid=obj.userid,
        site_license_flag=obj.site_license_flag,
        index_list=obj.index_list,
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
        # Who:
        **get_user()
    ))
    return event


def build_file_unique_id(doc):
    """Build file unique identifier."""
    key = '{0}_{1}_{2}_{3}_{4}_{5}_{6}'.format(
        doc['bucket_id'], doc['file_id'], doc['userrole'], doc['accessrole'],
        doc['index_list'], doc['site_license_flag'], doc['country']
    )
    doc['unique_id'] = str(uuid.uuid3(uuid.NAMESPACE_DNS, key))
    return doc


def build_record_unique_id(doc):
    """Build record unique identifier."""
    doc['unique_id'] = '{0}_{1}'.format(doc['record_id'], doc['country'])
    return doc


def record_view_event_builder(event, sender_app, pid=None, record=None,
                              **kwargs):
    """Build a record-view event."""
    # get index information
    index_list = []
    if len(record.navi) > 0:
        for index in record.navi:
            index_list.append(dict(
                index_id=index[1],
                index_name=index[3],
                index_name_en=index[4]
            ))

    event.update(dict(
        # When:
        timestamp=datetime.datetime.utcnow().isoformat(),
        # What:
        record_id=str(record.id),
        record_index_list=index_list,
        pid_type=pid.pid_type,
        pid_value=str(pid.pid_value),
        referrer=request.referrer,
        userid=record.userid,
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
    doc['unique_id'] = '{0}_{1}'.format("item", "create")
    return doc


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

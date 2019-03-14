# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Utilities for Invenio-Stats."""

from __future__ import absolute_import, print_function

import os
from base64 import b64encode

import six
from flask import current_app, request, session
from flask_login import current_user
from geolite2 import geolite2
from invenio_cache import current_cache
from werkzeug.utils import import_string

from invenio_search.api import RecordsSearch
from .proxies import current_stats


def get_anonymization_salt(ts):
    """Get the anonymization salt based on the event timestamp's day."""
    salt_key = 'stats:salt:{}'.format(ts.date().isoformat())
    salt = current_cache.get(salt_key)
    if not salt:
        salt_bytes = os.urandom(32)
        salt = b64encode(salt_bytes).decode('utf-8')
        current_cache.set(salt_key, salt, timeout=60 * 60 * 24)
    return salt


def get_geoip(ip):
    """Lookup country for IP address."""
    reader = geolite2.reader()
    ip_data = reader.get(ip) or {}
    return ip_data.get('country', {}).get('iso_code')


def get_user():
    """User information.

    .. note::

       **Privacy note** A users IP address, user agent string, and user id
       (if logged in) is sent to a message queue, where it is stored for about
       5 minutes. The information is used to:

       - Detect robot visits from the user agent string.
       - Generate an anonymized visitor id (using a random salt per day).
       - Detect the users host contry based on the IP address.

       The information is then discarded.
    """
    return dict(
        ip_address=request.remote_addr,
        user_agent=request.user_agent.string,
        user_id=(
            current_user.get_id() if current_user.is_authenticated else None
        ),
        session_id=session.get('sid_s')
    )


def obj_or_import_string(value, default=None):
    """Import string or return object.

    :params value: Import path or class object to instantiate.
    :params default: Default object to return if the import fails.
    :returns: The imported object.
    """
    if isinstance(value, six.string_types):
        return import_string(value)
    elif value:
        return value
    return default


def load_or_import_from_config(key, app=None, default=None):
    """Load or import value from config.

    :returns: The loaded value.
    """
    app = app or current_app
    imp = app.config.get(key)
    return obj_or_import_string(imp, default=default)


AllowAllPermission = type('Allow', (), {
    'can': lambda self: True,
    'allows': lambda *args: True,
})()


def default_permission_factory(query_name, params):
    """Default permission factory.

    It enables by default the statistics if they don't have a dedicated
    permission factory.
    """
    from invenio_stats import current_stats
    if current_stats.queries[query_name].permission_factory is None:
        return AllowAllPermission
    else:
        return current_stats.queries[query_name].permission_factory(
            query_name, params
        )


def build_record_stats(bucket_id):
    """Build the record's stats."""
    stats = {}
    stats_sources = {
    #     'bucket-file-download-histogram': {
    #         'params': {'recid': recid}, #recid
    #         'fields': {
    #             'views': 'count',
    #             'unique_views': 'unique_count',
    #         },
    #     },
        'bucket-file-download-total': {
            'params': {'bucket_id': bucket_id},
            'fields': {
                'downloads': 'count',
                'unique_downloads': 'unique_count',
                'volume': 'volume',
            },
        },
    #     'bucket-file-preview-histogram': {
    #         'params': {'conceptrecid': conceptrecid},
    #         'fields': {
    #             'version_views': 'count',
    #             'version_unique_views': 'unique_count',
    #         }
    #     },
        # 'bucket-file-preview-total': {
        #     'params': {'conceptrecid': conceptrecid},
        #     'fields': {
        #         'version_downloads': 'count',
        #         'version_unique_downloads': 'unique_count',
        #         'version_volume': 'volume',
        #     },
        # },
    #     'record-view': {
    #         'params': {'recid': recid},
    #         'fields': {
    #             'views': 'count',
    #             'unique_views': 'unique_count',
    #         },
    #     },
        # 'record-download': {
        #     'params': {'bucket_id': bucket_id},
        #     'fields': {
        #         'downloads': 'count',
        #         'unique_downloads': 'unique_count',
        #         'volume': 'volume',
        #     },
        # },
        # 'record-view-all-versions': {
        #     'params': {'conceptrecid': conceptrecid},
        #     'fields': {
        #         'version_views': 'count',
        #         'version_unique_views': 'unique_count',
        #     }
        # },
        # 'record-download-all-versions': {
        #     'params': {'conceptrecid': conceptrecid},
        #     'fields': {
        #         'version_downloads': 'count',
        #         'version_unique_downloads': 'unique_count',
        #         'version_volume': 'volume',
        #     },
        # },
    }
    for query_name, cfg in stats_sources.items():
        try:
            query_cfg = current_stats.queries[query_name]
            query = query_cfg.query_class(**query_cfg.query_config)
            result = query.run(**cfg['params'])
            for dst, src in cfg['fields'].items():
                stats[dst] = result.get(src)
        except Exception:
            pass
    return stats


def get_record_stats(recordid, throws=True):
    """Fetch record statistics from Elasticsearch."""
    try:
        res = (RecordsSearch()
               .source(include='_stats')  # only include "_stats" field
               .get_record(recordid)
               .execute())
        return res[0]._stats.to_dict() if res else None
    except Exception:
        if throws:
            raise
        pass

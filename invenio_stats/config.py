# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Proxy to the current stats module."""

from __future__ import absolute_import, print_function

from kombu import Exchange

from .utils import default_permission_factory

STATS_REGISTER_RECEIVERS = True
"""Enable the registration of signal receivers.

Default is ``True``.
The signal receivers are functions which will listen to the signals listed in
by the ``STATS_EVENTS`` config variable. An event will be generated for each
signal sent.
"""

PROVIDE_PERIOD_YEAR = 5

STATS_EVENTS = {
    'file-download': {
        'signal': 'invenio_files_rest.signals.file_downloaded',
        'event_builders': [
            'invenio_stats.contrib.event_builders.file_download_event_builder'
        ]
    },
    'file-preview': {
        'signal': 'invenio_files_rest.signals.file_previewed',
        'event_builders': [
            'invenio_stats.contrib.event_builders.file_preview_event_builder'
        ]
    },
    'item-create': {
        'signal': 'weko_deposit.signals.item_created',
        'event_builders': [
            'invenio_stats.contrib.event_builders.item_create_event_builder'
        ]
    },
    'record-view': {
        'signal': 'invenio_records_ui.signals.record_viewed',
        'event_builders': [
            'invenio_stats.contrib.event_builders.record_view_event_builder'
        ]
    },
    'top-view': {
        'signal': 'weko_theme.views.top_viewed',
        'event_builders': [
            'invenio_stats.contrib.event_builders.top_view_event_builder'
        ]
    },
    'search': {
        'signal': 'weko_search_ui.views.searched',
        'event_builders': [
            'invenio_stats.contrib.event_builders.search_event_builder'
        ]
    }
}
"""Enabled Events.

Each key is the name of an event. A queue will be created for each event.

If the dict of an event contains the ``signal`` key, and the config variable
``STATS_REGISTER_RECEIVERS`` is ``True``, a signal receiver will be registered.
Receiver function which will be connected on a signal and emit events. The key
is the name of the emitted event.

``signal``: Signal to which the receiver will be connected to.

``event_builders``: list of functions which will create and enhance the event.
    Each function will receive the event created by the previous function and
    can update it. Keep in mind that these functions will run synchronously
    during the creation of the event, meaning that if the signal is sent during
    a request they will increase the response time.
"""


STATS_AGGREGATIONS = {
    'file-download-agg': {},
    'file-preview-agg': {},
    'record-view-agg': {},
    'item-create-agg': {},
}


STATS_QUERIES = {
    'get-file-download-report': {},
    'get-file-download-open-access-report': {},
    'get-file-preview-report': {},
    'get-file-preview-open-access-report': {},
    'bucket-file-download-histogram': {},
    'bucket-file-download-total': {},
    'bucket-file-preview-histogram': {},
    'bucket-file-preview-total': {},
    'get-record-view-report':{},
    'bucket-record-view-histogram': {},
    'bucket-record-view-total': {},
    'item-create-histogram': {},
    'item-create-total': {},
    'item-create-host-total': {},
}


STATS_PERMISSION_FACTORY = default_permission_factory
"""Permission factory used by the statistics REST API.

This is a function which returns a permission granting or forbidding access
to a request. It is of the form ``permission_factory(query_name, params)``
where ``query_name`` is the name of the statistic requested by the user and
``params`` is a dict of parameters for this statistic. The result of the
function is a Permission.

See Invenio-access and Flask-principal for a better understanding of the
access control mechanisms.
"""


STATS_MQ_EXCHANGE = Exchange(
    'events',
    type='direct',
    delivery_mode='transient',  # in-memory queue
)
"""Default exchange used for the message queues."""

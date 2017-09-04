#!/usr/bin/env python
"""
    File 	        : query_result_view
    Package         :
    Description     :
    Project Name    : redash
    Created by Sabariram on 4/9/17
    Copyright (c) 2016 Knabfinance. All rights reserved.
"""

__author__ = "sabariram"
__version__ = '1.0'

import logging
import json
import time

import pystache
from flask import make_response, request
from flask_login import current_user
from flask_restful import abort
from redash import models, settings, utils
from redash.tasks import QueryTask, record_event
from redash.permissions import require_permission, not_view_only, has_access, require_access, view_only
from redash.handlers.base import BaseResource, get_object_or_404
from redash.utils import collect_query_parameters, collect_parameters_from_request, gen_query_hash
from redash.tasks.queries import enqueue_query


from query_results import QueryResultResource,error_response,run_query_sync,ONE_YEAR


#Sabari

class QueryResultViewResource(BaseResource):
    @staticmethod
    def add_cors_headers(headers):
        if 'Origin' in request.headers:
            origin = request.headers['Origin']

            if set(['*', origin]) & settings.ACCESS_CONTROL_ALLOW_ORIGIN:
                headers['Access-Control-Allow-Origin'] = origin
                headers['Access-Control-Allow-Credentials'] = str(settings.ACCESS_CONTROL_ALLOW_CREDENTIALS).lower()

    @require_permission('view_query_result')
    def options(self, query_id=None, query_result_id=None, filetype='json'):
        headers = {}
        self.add_cors_headers(headers)

        if settings.ACCESS_CONTROL_REQUEST_METHOD:
            headers['Access-Control-Request-Method'] = settings.ACCESS_CONTROL_REQUEST_METHOD

        if settings.ACCESS_CONTROL_ALLOW_HEADERS:
            headers['Access-Control-Allow-Headers'] = settings.ACCESS_CONTROL_ALLOW_HEADERS

        return make_response("", 200, headers)

    @require_permission('view_query_result')
    def get(self, query_id=None, query_result_id=None, filetype='json'):
        """
        Retrieve query results.

        :param number query_id: The ID of the query whose results should be fetched
        :param number query_result_id: the ID of the query result to fetch
        :param string filetype: Format to return. One of 'json', 'xlsx', or 'csv'. Defaults to 'json'.

        :<json number id: Query result ID
        :<json string query: Query that produced this result
        :<json string query_hash: Hash code for query text
        :<json object data: Query output
        :<json number data_source_id: ID of data source that produced this result
        :<json number runtime: Length of execution time in seconds
        :<json string retrieved_at: Query retrieval date/time, in ISO format
        """
        # TODO:
        # This method handles two cases: retrieving result by id & retrieving result by query id.
        # They need to be split, as they have different logic (for example, retrieving by query id
        # should check for query parameters and shouldn't cache the result).
        should_cache = query_result_id is not None

        parameter_values = collect_parameters_from_request(request.args)
        max_age = int(request.args.get('maxAge', 0))

        query_result = None

        if query_result_id:
            query_result = get_object_or_404(models.QueryResult.get_by_id_and_org, query_result_id, self.current_org)
        elif query_id is not None:
            query = get_object_or_404(models.Query.get_by_id_and_org, query_id, self.current_org)

            if query is not None:
                if settings.ALLOW_PARAMETERS_IN_EMBEDS and parameter_values:
                    query_result = run_query_sync(query.data_source, parameter_values, query.to_dict()['query'],
                                                  max_age=max_age)
                elif query.latest_query_data_id is not None:
                    query_result = get_object_or_404(models.QueryResult.get_by_id_and_org, query.latest_query_data_id,
                                                     self.current_org)

        if query_result:
            require_access(query_result.data_source.groups, self.current_user, view_only)

            if isinstance(self.current_user, models.ApiUser):
                event = {
                    'user_id': None,
                    'org_id': self.current_org.id,
                    'action': 'api_get',
                    'timestamp': int(time.time()),
                    'api_key': self.current_user.name,
                    'file_type': filetype,
                    'user_agent': request.user_agent.string,
                    'ip': request.remote_addr
                }

                if query_id:
                    event['object_type'] = 'query'
                    event['object_id'] = query_id
                else:
                    event['object_type'] = 'query_result'
                    event['object_id'] = query_result_id

                record_event.delay(event)

            response = self.make_json_response(query_result)

            if len(settings.ACCESS_CONTROL_ALLOW_ORIGIN) > 0:
                self.add_cors_headers(response.headers)

            if should_cache:
                response.headers.add_header('Cache-Control', 'max-age=%d' % ONE_YEAR)

            return response

        else:
            abort(404, message='No cached result found for this query.')

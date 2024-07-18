from __future__ import annotations

import os
from typing import Any, Mapping


url_templates = {
    'execution_status': 'https://api.dune.com/api/v1/execution/{execution_id}/status',
    'execution_results': 'https://api.dune.com/api/v1/execution/{execution_id}/results/csv',
    'query_execution': 'https://api.dune.com/api/v1/query/{query_id}/execute',
    'query_results': 'https://api.dune.com/api/v1/query/{query_id}/results/csv',
    'query_results_json': 'https://api.dune.com/api/v1/query/{query_id}/results',
    'query_create': 'https://api.dune.com/api/v1/query',
    'query': 'https://api.dune.com/api/v1/query/{query_id}',
}


def get_query_execute_url(query: int | str) -> str:
    if isinstance(query, str):
        return query
    elif isinstance(query, int):
        return url_templates['query_execution'].format(query_id=query)
    else:
        raise Exception('unknown query format: ' + str(type(query)))


def get_query_results_url(
    query: int | str, parameters: dict[str, Any], csv: bool = True
) -> str:
    query_id = get_query_id(query)
    if csv:
        template = url_templates['query_results']
    else:
        template = url_templates['query_results_json']
    url = template.format(query_id=query_id)

    parameters = dict(parameters.items())
    if 'query_parameters' in parameters:
        parameters['params'] = parameters.pop('query_parameters')
    for key, value in list(parameters.items()):
        if isinstance(value, dict):
            del parameters[key]
            for subkey, subvalue in value.items():
                parameters[key + '.' + subkey] = subvalue

    return add_args_to_url(url, parameters=parameters)


def get_execution_status_url(execution_id: str) -> str:
    return url_templates['execution_status'].format(execution_id=execution_id)


def get_execution_results_url(execution_id: str, parameters: Mapping[str, Any]) -> str:
    url = url_templates['execution_results'].format(execution_id=execution_id)
    return add_args_to_url(url, parameters=parameters)


def add_args_to_url(url: str, parameters: Mapping[str, Any]) -> str:
    url += '?'
    first = True
    for key, value in parameters.items():
        if value is None:
            continue
        if first:
            first = False
        else:
            url += '&'
        if isinstance(value, list):
            value = ','.join(str(item) for item in value)
        url += key + '=' + str(value)
    return url


def get_api_key() -> str:
    """get dune api key"""
    return os.environ['DUNE_API_KEY']


def get_query_id(query: str | int) -> int:
    """get id of a query"""
    if isinstance(query, int):
        return query
    elif query.startswith('https://api.dune.com/api/v1/query/'):
        query = query.split('https://api.dune.com/api/v1/query/')[1].split('/')[0]
    elif query.startswith('https://dune.com/queries'):
        query = query.split('https://dune.com/queries/')[1].split('/')[0]

    try:
        return int(query)
    except ValueError:
        raise Exception('invalid query id: ' + str(query))


def get_headers(*, api_key: str | None = None) -> Mapping[str, str]:
    if api_key is None:
        api_key = get_api_key()
    return {'X-Dune-API-Key': api_key}

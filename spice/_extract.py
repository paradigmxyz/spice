from __future__ import annotations

import asyncio
import io
import time
from typing import Any, Mapping, Optional, Sequence, TypedDict, cast
from typing_extensions import Unpack

import polars as pl

from ._types import Execution, Performance, Query
from . import _urls


class ExecuteKwargs(TypedDict):
    query_id: int | None
    api_key: str | None
    parameters: Mapping[str, Any] | None
    performance: Performance


class PollKwargs(TypedDict):
    api_key: str | None
    poll_interval: float


class ResultKwargs(TypedDict):
    limit: int | None
    offset: int | None
    sample_count: int | None
    sort_by: str | None
    columns: Sequence[str] | None
    extras: Mapping[str, Any] | None
    dtypes: Sequence[pl.PolarsDataType] | None


def query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    parameters: Mapping[str, Any] | None = None,
    api_key: str | None = None,
    performance: Performance = 'medium',
    poll: bool = True,
    poll_interval: float = 1.0,
    limit: int | None = None,
    offset: int | None = None,
    sample_count: int | None = None,
    sort_by: str | None = None,
    columns: Sequence[str] | None = None,
    extras: Mapping[str, Any] | None = None,
    dtypes: Sequence[pl.PolarsDataType] | None = None,
) -> pl.DataFrame | Execution:
    """get results of query as dataframe

    # Parameters
    - query: query or execution to retrieve results of
    - verbose: whether to print verbose info
    - refresh: trigger a new execution, or just use most recent execution
    - parameters: dict of query parameters
    - api_key: dune api key, otherwise use DUNE_API_KEY env var
    - performance: performance level
    - poll: wait for result as DataFrame, or just return Execution handle
    - poll_interval: polling interval in seconds
    - limit: number of rows to query in result
    - offset: row number to start returning results from
    - sample_count: number of random samples from query to return
    - sort_by: an ORDER BY clause to sort data by
    - columns: columns to retrieve, by default retrieve all columns
    - extras: extra parameters used for fetching execution result
        - examples: ignore_max_datapoints_per_request, allow_partial_results
    - dtypes: dtypes to use in output polars dataframe
    """

    # determine whether target is a query or an execution
    query_id, execution = _determine_input_type(query_or_execution)

    # gather arguments
    execute_kwargs: ExecuteKwargs = {
        'query_id': query_id,
        'api_key': api_key,
        'parameters': parameters,
        'performance': performance,
    }
    poll_kwargs: PollKwargs = {
        'poll_interval': poll_interval,
        'api_key': api_key,
    }
    result_kwargs: ResultKwargs = {
        'limit': limit,
        'offset': offset,
        'sample_count': sample_count,
        'sort_by': sort_by,
        'columns': columns,
        'extras': extras,
        'dtypes': dtypes,
    }

    # execute or retrieve query
    if query_id:
        if refresh:
            execution = _execute(**execute_kwargs)
        else:
            return _get_results(**execute_kwargs, **result_kwargs)

    # await execution completion
    if execution is None:
        raise Exception('could not determine execution')
    if poll:
        _poll_execution(execution, **poll_kwargs)
        return _get_results(execution, api_key, **result_kwargs)
    else:
        return execution


async def async_query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    parameters: Mapping[str, Any] | None = None,
    api_key: str | None = None,
    performance: Performance = 'medium',
    poll: bool = True,
    poll_interval: float = 1.0,
    limit: int | None = None,
    offset: int | None = None,
    sample_count: int | None = None,
    sort_by: str | None = None,
    columns: Sequence[str] | None = None,
    extras: Mapping[str, Any] | None = None,
    dtypes: Sequence[pl.PolarsDataType] | None = None,
) -> pl.DataFrame | Execution:
    """get results of query as dataframe

    ## Parameters
    - query: query or execution to retrieve results of
    - verbose: whether to print verbose info
    - refresh: trigger a new execution, or just use most recent execution
    - parameters: dict of query parameters
    - api_key: dune api key, otherwise use DUNE_API_KEY env var
    - performance: performance level
    - poll: wait for result as DataFrame, or just return Execution handle
    - poll_interval: polling interval in seconds
    - limit: number of rows to query in result
    - offset: row number to start returning results from
    - sample_count: number of random samples from query to return
    - sort_by: an ORDER BY clause to sort data by
    - columns: columns to retrieve, by default retrieve all columns
    - extras: extra parameters used for fetching execution result
        - examples: ignore_max_datapoints_per_request, allow_partial_results
    - dtypes: dtypes to use in output polars dataframe
    """

    # determine whether target is a query or an execution
    query_id, execution = _determine_input_type(query_or_execution)

    # gather arguments
    execute_kwargs: ExecuteKwargs = {
        'query_id': query_id,
        'api_key': api_key,
        'parameters': parameters,
        'performance': performance,
    }
    poll_kwargs: PollKwargs = {
        'poll_interval': poll_interval,
        'api_key': api_key,
    }
    result_kwargs: ResultKwargs = {
        'limit': limit,
        'offset': offset,
        'sample_count': sample_count,
        'sort_by': sort_by,
        'columns': columns,
        'extras': extras,
        'dtypes': dtypes,
    }

    # execute or retrieve query
    if query_id:
        if refresh:
            execution = await _async_execute(**execute_kwargs)
        else:
            return await _async_get_results(**execute_kwargs, **result_kwargs)

    # await execution completion
    if execution is None:
        raise Exception('could not determine execution')
    if poll:
        await _async_poll_execution(execution, **poll_kwargs)
        return await _async_get_results(execution, api_key, **result_kwargs)
    else:
        return execution


def _determine_input_type(
    query_or_execution: Query | Execution,
) -> tuple[int | None, Execution | None]:
    if isinstance(query_or_execution, (int, str)):
        query_id = _urls.get_query_id(query_or_execution)
        execution = None
    elif isinstance(query_or_execution, dict) and 'execution_id' in query_or_execution:
        query_id = None
        execution = query_or_execution
    else:
        raise Exception('input must be a query id, query url, or execution id')

    return query_id, execution


def _execute(
    query_id: int | None,
    api_key: str | None,
    parameters: Mapping[str, Any] | None,
    performance: Performance,
) -> Execution:
    import json
    import requests

    # process inputs
    if api_key is None:
        api_key = _urls.get_api_key()
    if query_id is None:
        raise Exception('must specify query to execute')
    url = _urls.get_query_execute_url(query_id)
    headers = {'X-Dune-API-Key': api_key}
    data = {'query_parameters': parameters, 'performance': performance}

    # get result
    response = requests.post(url, headers=headers, data=json.dumps(data))

    # process result
    result: Mapping[str, Any] = response.json()
    if 'error' in result:
        raise Exception(result['error'])
    else:
        return cast(Execution, result)


async def _async_execute(
    query_id: int | None,
    api_key: str | None,
    parameters: Mapping[str, Any] | None,
    performance: Performance,
) -> Execution:
    import aiohttp

    # process inputs
    if api_key is None:
        api_key = _urls.get_api_key()
    if query_id is None:
        raise Exception('must specify query to execute')
    url = _urls.get_query_execute_url(query_id)
    headers = {'X-Dune-API-Key': api_key}
    data = {'query_parameters': parameters, 'performance': performance}

    # get result
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=data) as response:
            result: Mapping[str, Any] = await response.json()

    # process result
    if 'error' in result:
        raise Exception(result['error'])
    else:
        return cast(Execution, result)


def _get_results(
    execution: Execution | None = None,
    api_key: str | None = None,
    *,
    query_id: int | None = None,
    parameters: Mapping[str, Any] | None = None,
    performance: Performance | None = None,
    **result_kwargs: Unpack[ResultKwargs],
) -> pl.DataFrame:
    import json
    import requests

    # process inputs
    if query_id is not None:
        url = _urls.get_query_results_url(query_id)
    elif execution:
        url = _urls.get_execution_results_url(execution['execution_id'])
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}
    data = dict(result_kwargs.items())
    if 'dtypes' in data:
        dtypes = cast(Optional[Sequence[pl.PolarsDataType]], data.pop('dtypes'))
    else:
        dtypes = None
    if performance is not None:
        data['performance'] = performance
    if parameters is not None:
        data['query_parameters'] = parameters

    # get result
    response = requests.post(url, headers=headers, data=json.dumps(data))

    # process result
    result = response.text
    return _process_raw_table(result, dtypes=dtypes)


async def _async_get_results(
    execution: Execution | None = None,
    api_key: str | None = None,
    *,
    query_id: int | None = None,
    parameters: Mapping[str, Any] | None = None,
    performance: Performance | None = None,
    **result_kwargs: Unpack[ResultKwargs],
) -> pl.DataFrame:
    import aiohttp

    # process inputs
    if query_id is not None:
        url = _urls.get_query_results_url(query_id)
    elif execution:
        url = _urls.get_execution_results_url(execution['execution_id'])
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}
    data = dict(result_kwargs.items())
    if 'dtypes' in data:
        dtypes = cast(Optional[Sequence[pl.PolarsDataType]], data.pop('dtypes'))
    else:
        dtypes = None
    if parameters is not None:
        data['query_parameters'] = parameters

    # get result
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=data) as response:
            result: str = await response.text()

    # process result
    return _process_raw_table(result, dtypes=dtypes)


def _process_raw_table(
    raw_csv: str,
    dtypes: Sequence[pl.PolarsDataType] | None,
) -> pl.DataFrame:
    return pl.read_csv(
        io.StringIO(raw_csv),
        infer_schema_length=len(raw_csv),
        null_values='<nil>',
        truncate_ragged_lines=True,
        schema_overrides=dtypes,
    )


def _poll_execution(
    execution: Execution,
    *,
    api_key: str | None,
    poll_interval: float,
) -> None:
    import requests

    # process inputs
    url = _urls.get_execution_status_url(execution['execution_id'])
    execution_id = execution['execution_id']
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}

    # poll until completion
    while True:
        t_poll = time.time()
        response = requests.get(url, headers=headers)
        result = response.json()
        if result['is_execution_finished']:
            break

        # wait until polling interval
        t_wait = time.time() - t_poll
        if t_wait > 0:
            time.sleep(t_wait)

    # check for errors
    if result['state'] == 'QUERY_STATE_FAILED':
        raise Exception('QUERY FAILED execution_id={}'.format(execution_id))


async def _async_poll_execution(
    execution: Execution,
    *,
    api_key: str | None,
    poll_interval: float,
) -> None:
    import aiohttp

    # process inputs
    url = _urls.get_execution_status_url(execution['execution_id'])
    execution_id = execution['execution_id']
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}

    # poll until completion
    async with aiohttp.ClientSession() as session:
        while True:
            t_poll = time.time()
            async with session.get(url, headers=headers) as response:
                result = await response.json()
                if result['is_execution_finished']:
                    break

            # wait until polling interval
            t_wait = time.time() - t_poll
            if t_wait > 0:
                await asyncio.sleep(t_wait)

    # check for errors
    if result['state'] == 'QUERY_STATE_FAILED':
        raise Exception('QUERY FAILED execution_id={}'.format(execution_id))

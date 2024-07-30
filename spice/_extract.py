from __future__ import annotations

import asyncio
import io
import time
from typing import (
    Any,
    Literal,
    Mapping,
    Optional,
    Sequence,
    TypedDict,
    cast,
    overload,
)
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
    verbose: bool


class ResultKwargs(TypedDict):
    limit: int | None
    offset: int | None
    sample_count: int | None
    sort_by: str | None
    columns: Sequence[str] | None
    extras: Mapping[str, Any] | None
    dtypes: Sequence[type[pl.DataType]] | Mapping[str, type[pl.DataType]] | None
    verbose: bool


@overload
def query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    max_age: int | float | None = None,
    parameters: Mapping[str, Any] | None = None,
    api_key: str | None = None,
    performance: Performance = 'medium',
    poll: Literal[True] = True,
    poll_interval: float = 1.0,
    limit: int | None = None,
    offset: int | None = None,
    sample_count: int | None = None,
    sort_by: str | None = None,
    columns: Sequence[str] | None = None,
    extras: Mapping[str, Any] | None = None,
    dtypes: Sequence[type[pl.DataType]] | Mapping[str, type[pl.DataType]] | None = None,
) -> pl.DataFrame | Execution: ...


@overload
def query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    max_age: int | float | None = None,
    parameters: Mapping[str, Any] | None = None,
    api_key: str | None = None,
    performance: Performance = 'medium',
    poll: Literal[False],
    poll_interval: float = 1.0,
    limit: int | None = None,
    offset: int | None = None,
    sample_count: int | None = None,
    sort_by: str | None = None,
    columns: Sequence[str] | None = None,
    extras: Mapping[str, Any] | None = None,
    dtypes: Sequence[type[pl.DataType]] | Mapping[str, type[pl.DataType]] | None = None,
) -> pl.DataFrame | Execution: ...


def query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    max_age: int | float | None = None,
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
    dtypes: Sequence[type[pl.DataType]] | Mapping[str, type[pl.DataType]] | None = None,
) -> pl.DataFrame | Execution:
    """get results of query as dataframe

    # Parameters
    - query: query or execution to retrieve results of
    - verbose: whether to print verbose info
    - refresh: trigger a new execution, or just use most recent execution
    - max_age: max age of last execution in seconds, or trigger a new execution
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
        'verbose': verbose,
    }
    result_kwargs: ResultKwargs = {
        'limit': limit,
        'offset': offset,
        'sample_count': sample_count,
        'sort_by': sort_by,
        'columns': columns,
        'extras': extras,
        'dtypes': dtypes,
        'verbose': verbose,
    }

    # execute or retrieve query
    if query_id:
        if max_age is not None and not refresh:
            age = _get_query_latest_age(**execute_kwargs, verbose=verbose)  # type: ignore
            if age is None or age > max_age:
                refresh = True
        if not refresh:
            df = _get_results(**execute_kwargs, **result_kwargs)
            if df is not None:
                return df
        execution = _execute(**execute_kwargs, verbose=verbose)

    # await execution completion
    if execution is None:
        raise Exception('could not determine execution')
    if poll:
        _poll_execution(execution, **poll_kwargs)
        df = _get_results(execution, api_key, **result_kwargs)
        if df is not None:
            return df
        else:
            raise Exception('no successful execution for query')
    else:
        return execution


@overload
async def async_query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    max_age: int | float | None = None,
    parameters: Mapping[str, Any] | None = None,
    api_key: str | None = None,
    performance: Performance = 'medium',
    poll: Literal[True] = True,
    poll_interval: float = 1.0,
    limit: int | None = None,
    offset: int | None = None,
    sample_count: int | None = None,
    sort_by: str | None = None,
    columns: Sequence[str] | None = None,
    extras: Mapping[str, Any] | None = None,
    dtypes: Sequence[type[pl.DataType]] | Mapping[str, type[pl.DataType]] | None = None,
) -> pl.DataFrame: ...


@overload
async def async_query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    max_age: int | float | None = None,
    parameters: Mapping[str, Any] | None = None,
    api_key: str | None = None,
    performance: Performance = 'medium',
    poll: Literal[False],
    poll_interval: float = 1.0,
    limit: int | None = None,
    offset: int | None = None,
    sample_count: int | None = None,
    sort_by: str | None = None,
    columns: Sequence[str] | None = None,
    extras: Mapping[str, Any] | None = None,
    dtypes: Sequence[type[pl.DataType]] | Mapping[str, type[pl.DataType]] | None = None,
) -> Execution: ...


async def async_query(
    query_or_execution: Query | Execution,
    *,
    verbose: bool = True,
    refresh: bool = False,
    max_age: int | float | None = None,
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
    dtypes: Sequence[type[pl.DataType]] | Mapping[str, type[pl.DataType]] | None = None,
) -> pl.DataFrame | Execution:
    """get results of query as dataframe

    ## Parameters
    - query: query or execution to retrieve results of
    - verbose: whether to print verbose info
    - refresh: trigger a new execution, or just use most recent execution
    - max_age: max age of last execution in seconds, or trigger a new execution
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
        'verbose': verbose,
    }
    result_kwargs: ResultKwargs = {
        'limit': limit,
        'offset': offset,
        'sample_count': sample_count,
        'sort_by': sort_by,
        'columns': columns,
        'extras': extras,
        'dtypes': dtypes,
        'verbose': verbose,
    }

    # execute or retrieve query
    if query_id:
        if max_age is not None and not refresh:
            age = await _async_get_query_latest_age(**execute_kwargs, verbose=verbose)  # type: ignore
            if age is None or age > max_age:
                refresh = True
        if not refresh:
            df = await _async_get_results(**execute_kwargs, **result_kwargs)
            if df is not None:
                return df
        execution = await _async_execute(**execute_kwargs, verbose=verbose)

    # await execution completion
    if execution is None:
        raise Exception('could not determine execution')
    if poll:
        await _async_poll_execution(execution, **poll_kwargs)
        df = await _async_get_results(execution, api_key, **result_kwargs)
        if df is not None:
            return df
        else:
            raise Exception('no successful execution for query')
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


def _get_query_latest_age(
    query_id: int,
    *,
    verbose: bool = True,
    parameters: Mapping[str, Any] | None = None,
    performance: Performance = 'medium',
    api_key: str | None = None,
) -> float | None:
    import datetime
    import json
    import requests

    # process inputs
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}
    data = {}
    if parameters is not None:
        data['query_parameters'] = parameters
    url = _urls.get_query_results_url(query_id, parameters=data, csv=False)

    # print summary
    if verbose:
        print('checking age of last execution, query_id = ' + str(query_id))

    # perform request
    response = requests.get(url, headers=headers)

    # check if result is error
    result = response.json()
    try:
        if 'error' in result:
            if (
                result['error']
                == 'not found: No execution found for the latest version of the given query'
            ):
                if verbose:
                    print('no age for query, because no previous executions exist')
                return None
            raise Exception(result['error'])
    except json.JSONDecodeError:
        pass

    # process result
    if 'execution_started_at' in result:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        started = (
            datetime.datetime.strptime(
                result['execution_started_at'],
                '%Y-%m-%dT%H:%M:%S.%fZ',
            )
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )
        age = now - started

        if verbose:
            print('latest result age:', age)

        return age
    else:
        if verbose:
            print('no age for query, because no previous executions exist')
        return None


async def _async_get_query_latest_age(
    query_id: int,
    *,
    verbose: bool = True,
    parameters: Mapping[str, Any] | None = None,
    performance: Performance = 'medium',
    api_key: str | None = None,
) -> float | None:
    import datetime
    import json
    import aiohttp

    # process inputs
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}
    data = {}
    if parameters is not None:
        data['query_parameters'] = parameters
    url = _urls.get_query_results_url(query_id, parameters=data, csv=False)

    # print summary
    if verbose:
        print('checking age of last execution, query_id = ' + str(query_id))

    # perform request
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            result: Mapping[str, Any] = await response.json()

    # check if result is error
    try:
        if 'error' in result:
            if (
                result['error']
                == 'not found: No execution found for the latest version of the given query'
            ):
                if verbose:
                    print('no age for query, because no previous executions exist')
                return None
            raise Exception(result['error'])
    except json.JSONDecodeError:
        pass

    # process result
    if 'execution_started_at' in result:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        started = (
            datetime.datetime.strptime(
                result['execution_started_at'],
                '%Y-%m-%dT%H:%M:%S.%fZ',
            )
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
        )
        age = now - started

        if verbose:
            print('latest result age:', age)

        return age
    else:
        if verbose:
            print('no age for query, because no previous executions exist')
        return None


def _execute(
    query_id: int | None,
    api_key: str | None,
    parameters: Mapping[str, Any] | None,
    performance: Performance,
    verbose: bool,
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
    data = {k: v for k, v in data.items() if v is not None}

    # print summary
    if verbose:
        print('initiating new execution of query_id = ' + str(query_id))
        if verbose >= 2:
            print('execution url = ' + url)
            print('execution parameters = ' + str(parameters))

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
    verbose: bool,
) -> Execution:
    import aiohttp
    import json

    # process inputs
    if api_key is None:
        api_key = _urls.get_api_key()
    if query_id is None:
        raise Exception('must specify query to execute')
    url = _urls.get_query_execute_url(query_id)
    headers = {'X-Dune-API-Key': api_key}
    data = {'query_parameters': parameters, 'performance': performance}
    data = {k: v for k, v in data.items() if v is not None}

    # print summary
    if verbose:
        print('initiating new execution of query_id = ' + str(query_id))
        if verbose >= 2:
            print('execution url = ' + url)
            print('execution parameters = ' + str(parameters))

    # get result
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, headers=headers, data=json.dumps(data)
        ) as response:
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
) -> pl.DataFrame | None:
    import json
    import requests

    # process inputs
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}
    data = dict(result_kwargs.items())
    if 'dtypes' in data:
        dtypes = cast(Optional[Sequence[type[pl.DataType]]], data.pop('dtypes'))
    else:
        dtypes = None
    if 'verbose' in data:
        verbose = data.pop('verbose')
    else:
        verbose = False
    if performance is not None:
        data['performance'] = performance
    if parameters is not None:
        data['query_parameters'] = parameters
    if query_id is not None:
        url = _urls.get_query_results_url(query_id, parameters=data)
    elif execution:
        url = _urls.get_execution_results_url(
            execution['execution_id'], parameters=data
        )
    else:
        raise Exception('must specify query_id or execution')

    # print summary
    if verbose:
        if query_id is not None:
            print('getting results, query_id = ' + str(query_id))
        elif execution:
            print('getting results, execution_id = ' + str(execution['execution_id']))
        if verbose >= 2:
            print('results url = ' + str(url))

    # get result
    response = requests.get(url, headers=headers)

    # process result
    try:
        as_json = response.json()
        if 'error' in as_json:
            if (
                as_json['error']
                == 'not found: No execution found for the latest version of the given query'
            ):
                if verbose:
                    msg = 'no existing execution for query_id = {query_id}, initializing new execution'.format(
                        query_id=str(query_id)
                    )
                    if parameters is None or len(parameters) == 0:
                        msg = msg + ' with no parameters'
                    else:
                        str_parameters = str(parameters)
                        if len(str_parameters) > 100:
                            str_parameters = str_parameters[:97] + '...'
                        msg = msg + ' with parameters = ' + str_parameters
                    print(msg)
                return None
            raise Exception(as_json['error'])
    except json.JSONDecodeError:
        pass
    result = response.text
    df = _process_raw_table(result, dtypes=dtypes)

    # get all pages
    limit = result_kwargs.get('limit')
    if limit is not None:
        n_rows = len(df)
        pages = []
        while 'x-dune-next-uri' in response.headers and n_rows < limit:
            if verbose:
                offset = response.headers['x-dune-next-offset']
                print('gathering additional page, offset = ' + str(offset))
            url = response.headers['x-dune-next-uri']
            response = requests.get(url, headers=headers)
            page = _process_raw_table(response.text, dtypes=dtypes)
            n_rows += len(page)
            pages.append(page)
        df = pl.concat([df, *pages]).limit(limit)

    return df


async def _async_get_results(
    execution: Execution | None = None,
    api_key: str | None = None,
    *,
    query_id: int | None = None,
    parameters: Mapping[str, Any] | None = None,
    performance: Performance | None = None,
    **result_kwargs: Unpack[ResultKwargs],
) -> pl.DataFrame | None:
    import json
    import aiohttp

    # process inputs
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}
    data = dict(result_kwargs.items())
    if 'dtypes' in data:
        dtypes = cast(Optional[Sequence[type[pl.DataType]]], data.pop('dtypes'))
    else:
        dtypes = None
    if 'verbose' in data:
        verbose = data.pop('verbose')
    else:
        verbose = False
    if parameters is not None:
        data['query_parameters'] = parameters
    if query_id is not None:
        url = _urls.get_query_results_url(query_id, parameters=data)
    elif execution:
        url = _urls.get_execution_results_url(
            execution['execution_id'], parameters=data
        )
    else:
        raise Exception('must specify query_id or execution')

    # print summary
    if verbose:
        if query_id is not None:
            print('getting results, query_id = ' + str(query_id))
        elif execution:
            print('getting results, execution_id = ' + str(execution['execution_id']))
        if verbose >= 2:
            print('results url = ' + str(url))

    # get result
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            result: str = await response.text()
            response_headers = response.headers

    # process result
    try:
        as_json = json.loads(result)
        if 'error' in as_json:
            if (
                as_json['error']
                == 'not found: No execution found for the latest version of the given query'
            ):
                if verbose:
                    msg = 'no existing execution for query_id = {query_id}, initializing new execution'.format(
                        query_id=str(query_id)
                    )
                    if parameters is None or len(parameters) == 0:
                        msg = msg + ' with no parameters'
                    else:
                        str_parameters = str(parameters)
                        if len(str_parameters) > 100:
                            str_parameters = str_parameters[:97] + '...'
                        msg = msg + ' with parameters = ' + str_parameters
                    print(msg)
                return None
            raise Exception(as_json['error'])
    except json.JSONDecodeError:
        pass
    df = _process_raw_table(result, dtypes=dtypes)

    # get all pages
    limit = result_kwargs.get('limit')
    if limit is not None:
        n_rows = len(df)
        pages = []
        async with aiohttp.ClientSession() as session:
            while 'x-dune-next-uri' in response_headers and n_rows < limit:
                if verbose:
                    offset = response.headers['x-dune-next-offset']
                    print('gathering additional page, offset = ' + str(offset))
                url = response_headers['x-dune-next-uri']
                async with session.get(url, headers=headers) as response:
                    result = await response.text()
                    response_headers = response.headers
                page = _process_raw_table(result, dtypes=dtypes)
                n_rows += len(page)
                pages.append(page)

        df = pl.concat([df, *pages]).limit(limit)

    return df


def _process_raw_table(
    raw_csv: str,
    dtypes: Sequence[type[pl.DataType] | None]
    | Mapping[str, type[pl.DataType] | None]
    | None,
) -> pl.DataFrame:
    # convert from map to sequence
    first_line = raw_csv.split('\n', maxsplit=1)[0]
    column_order = first_line.split(',')

    # parse data as csv
    df = pl.read_csv(
        io.StringIO(raw_csv),
        infer_schema_length=len(raw_csv),
        null_values='<nil>',
        truncate_ragged_lines=True,
        schema_overrides=[pl.String for column in column_order],
    )

    # cast types
    new_dtypes = []
    for c, column in enumerate(df.columns):
        new_dtype = None
        if dtypes is not None:
            if isinstance(dtypes, list):
                if len(dtypes) > c and dtypes[c] is not None:
                    new_dtype = dtypes[c]
            elif isinstance(dtypes, dict):
                if column in dtypes and dtypes[column] is not None:
                    new_dtype = dtypes[column]
            else:
                raise Exception('invalid format for dtypes')

        if new_dtype is None:
            new_dtype = infer_dtype(df[column])

        if new_dtype == pl.Datetime or isinstance(new_dtype, pl.Datetime):
            time_format = '%Y-%m-%d %H:%M:%S%.3f %Z'
            df = df.with_columns(pl.col(column).str.to_datetime(time_format))
            new_dtype = None

        new_dtypes.append(new_dtype)

    new_columns = []
    for column, dtype in zip(df.columns, new_dtypes):
        if dtype is not None:
            if dtype == pl.Boolean:
                new_column = pl.col(column) == 'true'
            else:
                new_column = pl.col(column).cast(dtype)
            new_columns.append(new_column)
    df = df.with_columns(*new_columns)

    return df


def infer_dtype(s: pl.Series) -> pl.DataType:
    try:
        as_str = pl.DataFrame(s).write_csv(None)
        return pl.read_csv(io.StringIO(as_str))[s.name].dtype
    except Exception:
        return pl.String()


def _poll_execution(
    execution: Execution,
    *,
    api_key: str | None,
    poll_interval: float,
    verbose: bool,
) -> None:
    import requests

    # process inputs
    url = _urls.get_execution_status_url(execution['execution_id'])
    execution_id = execution['execution_id']
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}

    # print summary
    t_start = time.time()

    # poll until completion
    while True:
        t_poll = time.time()

        # print summary
        if verbose:
            print(
                'polling results, execution_id = '
                + str(execution['execution_id'])
                + ', t = '
                + str(t_poll - t_start)
            )

        # poll
        response = requests.get(url, headers=headers)
        result = response.json()
        if result['is_execution_finished']:
            break

        # wait until polling interval
        t_wait = time.time() - t_poll
        if t_wait < poll_interval:
            time.sleep(poll_interval - t_wait)

    # check for errors
    if result['state'] == 'QUERY_STATE_FAILED':
        raise Exception('QUERY FAILED execution_id={}'.format(execution_id))


async def _async_poll_execution(
    execution: Execution,
    *,
    api_key: str | None,
    poll_interval: float,
    verbose: bool,
) -> None:
    import aiohttp

    # process inputs
    url = _urls.get_execution_status_url(execution['execution_id'])
    execution_id = execution['execution_id']
    if api_key is None:
        api_key = _urls.get_api_key()
    headers = {'X-Dune-API-Key': api_key}

    # print summary
    t_start = time.time()

    # poll until completion
    async with aiohttp.ClientSession() as session:
        while True:
            t_poll = time.time()

            # print summary
            if verbose:
                print(
                    'polling results, execution_id = '
                    + str(execution['execution_id'])
                    + ', t = '
                    + str(t_poll - t_start)
                )

            # poll
            async with session.get(url, headers=headers) as response:
                result = await response.json()
                if result['is_execution_finished']:
                    break

            # wait until polling interval
            t_wait = time.time() - t_poll
            if t_wait < poll_interval:
                await asyncio.sleep(poll_interval - t_wait)

    # check for errors
    if result['state'] == 'QUERY_STATE_FAILED':
        raise Exception('QUERY FAILED execution_id={}'.format(execution_id))

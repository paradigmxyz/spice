from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil

import polars as pl

from . import _extract
from ._types import Execution


cache_template = '{query_id}__{execution_id}__{parameter_hash}__{timestamp}.parquet'


def load_from_cache(
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.ResultKwargs,
    cache_dir: str | None,
) -> pl.DataFrame | None:

    # get latest execution id
    execution = _extract.get_latest_execution(execute_kwargs)
    if execution is None:
        return None

    # build cache path
    cache_path = _build_cache_path(
        execution=execution,
        execute_kwargs=execute_kwargs,
        result_kwargs=result_kwargs,
        cache_dir=cache_dir,
    )

    # load cache result
    if os.path.exists(cache_path):
        if result_kwargs['verbose']:
            print('loading result from cache')
        return pl.read_parquet(cache_path)
    else:
        return None


async def async_load_from_cache(
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.ResultKwargs,
    cache_dir: str | None,
) -> pl.DataFrame | None:

    # get latest execution
    execution = await _extract.async_get_latest_execution(execute_kwargs)
    if execution is None:
        return None

    # build cache path
    cache_path = _build_cache_path(
        execution=execution,
        execute_kwargs=execute_kwargs,
        result_kwargs=result_kwargs,
        cache_dir=cache_dir,
    )

    # load cache result
    if os.path.exists(cache_path):
        if result_kwargs['verbose']:
            print('loading result from cache')
        return pl.read_parquet(cache_path)
    else:
        return None


def save_to_cache(
    df: pl.DataFrame,
    execution: Execution,
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.ResultKwargs,
    cache_dir: str | None,
) -> None:

    if result_kwargs['verbose']:
        print('saving result to cache')

    # build cache path
    cache_path = _build_cache_path(
        execution=execution,
        execute_kwargs=execute_kwargs,
        result_kwargs=result_kwargs,
        cache_dir=cache_dir,
    )

    # create dir
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    # save to cache
    tmp_path = cache_path + '_tmp_' + secrets.token_hex(8)  # add for if running in parallel
    df.write_parquet(tmp_path)
    shutil.move(tmp_path, cache_path)


def _build_cache_path(
    execution: Execution,
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.ResultKwargs,
    cache_dir: str | None,
) -> str:

    # get parameter hash
    if result_kwargs['dtypes'] is None:
        dtypes: list[str | list[str]] | None = None
    else:
        dtypes = []
        if isinstance(result_kwargs['dtypes'], list):
            for dtype in result_kwargs['dtypes']:
                dtypes.append(str(dtype))
        elif isinstance(result_kwargs['dtypes'], dict):
            for name, dtype in result_kwargs['dtypes'].items():
                dtypes.append([name, str(dtype)])
        else:
            raise Exception('invalid format for dtypes')
    hash_params = {
        'execution_id': execution['execution_id'],
        'query_id': execute_kwargs['query_id'],
        'parameters': execute_kwargs['parameters'],
        'limit': result_kwargs['limit'],
        'offset': result_kwargs['offset'],
        'sample_count': result_kwargs['sample_count'],
        'sort_by': result_kwargs['sort_by'],
        'columns': result_kwargs['columns'],
        'extras': result_kwargs['extras'],
        'dtypes': dtypes,
    }
    md5_hash = hashlib.md5()
    md5_hash.update(json.dumps(hash_params, sort_keys=True).encode('utf-8'))
    hash_str = md5_hash.hexdigest()[:16]

    # build from template
    timestamp = execution['timestamp']
    if timestamp is None:
        raise Exception('need completion timestamp on execution')
    cache_filename = cache_template.format(
        query_id=execute_kwargs['query_id'],
        execution_id=execution['execution_id'],
        parameter_hash=hash_str,
        timestamp=int(timestamp),
    )

    if cache_dir is None:
        cache_dir = '/tmp/dune_spice'

    return os.path.join(cache_dir, cache_filename)

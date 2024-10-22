# spice 🌶️

Simple client for extracting data from the [Dune Analytics API](https://docs.dune.com/api-reference/overview/introduction)

Goals of `spice`:
- use as a python library or as a CLI tool
- simple, no OOP, entire API is just one function
- support both sync and async workflows
- tight integration with [polars](https://github.com/pola-rs/polars)

To discuss `spice`, head to the [Paradigm Data Tools](https://t.me/paradigm_data) Telegram channel.

## Table of Contents
1. [Installation](#installation)
2. [Examples](#examples)
    1. [Sync Workflow](#sync-workflow)
    2. [Async Workflow](#async-workflow)
    3. [Command Line Workflow](#command-line-workflow)
    4. [Quality of Life](#quality-of-life)
3. [API Reference](#api-reference)
    1. [Python Reference](#python-reference)
    2. [Command Line Reference](#command-line-reference)
4. [FAQ](#faq)

## Installation

`pip install dune_spice`

## Examples

Can either use the sync workflow or async workflow. Each workflow has only one function.

See [API Reference](#api-reference) below for the full list of query function arguments.

### Sync Workflow

```python
import spice

# get most recent query results using query id
df = spice.query(21693)

# get most recent query results using query url
df = spice.query('https://dune.com/queries/21693')

# get most recent query results using raw sql
df = spice.query('SELECT * FROM ethereum.blocks LIMIT 5')

# perform new query execution and get results
df = spice.query(query, refresh=True)

# get query results for input parameters
df = spice.query(query, parameters={'network': 'ethereum'})

# perform new query execution, but do not wait for result
execution = spice.query(query, poll=False)

# get results of previous execution
df = spice.query(execution)
```

### Async Workflow

The async API is identical to the sync API as above, just add `async_` prefix.

```python
df = await spice.async_query(21693)
df = await spice.async_query('https://dune.com/queries/21693')
df = await spice.async_query('SELECT * FROM ethereum.blocks LIMIT 5')
df = await spice.async_query(query, refresh=True)
df = await spice.async_query(query, parameters={'network': 'ethereum'})
execution = spice.query(query, poll=False)
df = await spice.async_query(execution)
```

### Command Line Workflow

Running the `spice` CLI will 1) extract the given query, 2) preview the contents, 3) and save it to a file. Each step of this process can be customized using the CLI options.

```bash
spice 21693
spice https://dune.com/queries/21693
spice "SELECT * FROM ethereum.blocks LIMIT 5"
spice $QUERY --refresh
spice $QUERY --parameters network=ethereum
```

### Quality of Life

`spice` contains additional quality of life features such as:
- automatically cache results locally to save time and credits for repeated queries
- automatically handle pagination of multi-page results
- automatically execute queries that have no existing executions, especially when using new parameter values
- allow type overrides using the `types` parameter
- support raw dynamic querying using parameter templates in order to 1) minimize roundtrips and 2) not require a paid API plan
- auto-retry with exponential backoff when encountering HTTP429 ratelimit errors

## API Reference

#### Python Reference

These python functions are accessed as `spice.query()` and `spice.async_query()`.

```python
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
    types: Sequence[pl.DataType] | None = None,
    cache: bool = True,
    cache_dir: str | None = None,
    save_to_cache: bool = True,
    load_from_cache: bool = True,
) -> pl.DataFrame | Execution:
    """get results of query as dataframe

    # Parameters
    - query_or_execution: query or execution to retrieve results of
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
    - types: column types to use in output polars dataframe
    - cache: whether to use cache for saving or loading
    - cache_dir: directory to use for cached data (create tmp_dir if None)
    - save_to_cache: whether to save to cache, set false to load only
    - load_from_cache: whether to load from cache, set false to save only
    - include_execution: return Execution metadata alongside query result
    """
    ...

async def async_query(
    # all the same parameters as query()
    ...
) -> pl.DataFrame | Execution:
    """get results of query as dataframe, asynchronously

    ## Parameters
    [see query()]
    """
    ...
```

#### Command Line Reference

![image](https://github.com/user-attachments/assets/aca9c2a1-1487-461f-ab2f-8e7a4930b71a)


## FAQ

#### How do I set my Dune API key?
`spice` looks for a Dune API key in the `DUNE_API_KEY` environment variable.

#### Does `spice` work with a Dune free account?

Yes. But to fetch the result of large queries, you may need to remove the "250k datapoints per request" limitation in your account settings on the Dune website.

#### Which endpoints does this package support?
`spice` interacts only with Dune's SQL-related API endpoints, documented [here](https://docs.dune.com/api-reference/executions/execution-object).

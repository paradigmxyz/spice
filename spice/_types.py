from __future__ import annotations

from typing import Literal, TypedDict, Union


# query is an int id or query url
Query = Union[int, str]

# execution performance level
Performance = Literal['medium', 'large']


# execution
class Execution(TypedDict):
    execution_id: str

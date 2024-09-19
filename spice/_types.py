from __future__ import annotations

from typing import Literal, TypedDict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import NotRequired


# query is an int id or query url
Query = Union[int, str]

# execution performance level
Performance = Literal['medium', 'large']

# execution
class Execution(TypedDict):
    execution_id: str
    timestamp: NotRequired[int | None]

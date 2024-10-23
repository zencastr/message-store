from typing import Callable, Any, Coroutine, TypeVar, cast
import asyncio
import traceback
import inspect
import itertools
from .message_store_logger import message_store_logger

T = TypeVar("T")

async def retry_with_exponential_backoff(
    fn: Callable[[], T | Coroutine[Any, Any, T]],
    is_retriable: Callable[[Exception], bool],
    max_retries: int = 3,
    initial_backoff_time_in_seconds: float = 0.25,
) -> T:
    """
    Retries the given function with exponential backoff
    """
    current_backoff_time_in_seconds = initial_backoff_time_in_seconds
    for i in itertools.count():
        try:
            return_value = fn()
            if asyncio.iscoroutine(return_value):
                return await return_value
            else:
                return_value = cast(T, return_value)
                return return_value
        except Exception as e:
            if not is_retriable(e):
                raise
            if i >= max_retries - 1:
                raise
            fn_source = inspect.getsource(fn)
            message_store_logger.warning(
                f"{fn_source} failed. Retrying after {current_backoff_time_in_seconds} seconds (retry #{i + 1}/{max_retries})\n",
                traceback.format_exc(),
            )
            await asyncio.sleep(current_backoff_time_in_seconds)
            current_backoff_time_in_seconds *= 2

    raise RuntimeError("retry_with_exponential_backoff: exited loop without returning or raising an exception")
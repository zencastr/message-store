from typing import Callable, Any
from asyncio import sleep, iscoroutinefunction
import traceback
import inspect
from .message_store_logger import message_store_logger


async def retry_with_exponential_backoff(
    fn: Callable[[], Any],
    is_retriable: Callable[[Exception], bool],
    max_retries: int = 3,
    initial_backoff_time_in_seconds: float = 0.25,
):
    """
    Retries the given function with exponential backoff
    """
    current_backoff_time_in_seconds = initial_backoff_time_in_seconds
    for i in range(max_retries):
        try:
            if iscoroutinefunction(fn):
                return await fn()
            else:
                return fn()
        except Exception as e:
            if not is_retriable(e):
                raise
            if i == max_retries - 1:
                raise
            fn_source = inspect.getsource(fn)
            message_store_logger.warning(
                f"MessageStore: {fn_source} failed. Retrying after {current_backoff_time_in_seconds} seconds (retry #{i + 1}/{max_retries})\n",
                traceback.format_exc(),
            )
            await sleep(current_backoff_time_in_seconds)
            current_backoff_time_in_seconds *= 2

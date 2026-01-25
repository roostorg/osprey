from typing import Any, Optional, Union

from typing_extensions import NotRequired, TypedDict


class PubSubTaskMessageData(TypedDict):
    task_name: str
    old_task_name: NotRequired[Optional[str]]
    task_args: Union[list, tuple]  # type: ignore[type-arg]
    task_kwargs: dict[str, Any]
    # The unix timestamps (millis) at which the message was created, even
    # before publishing to pubsub.
    created_at_ms: Optional[int]

    # Optionally, one can specify a delay in milliseconds relative to created_at_ms. If set,
    # this will make the subscriber worker keep delaying doing the task until delay_ms have passed since created_at_ms.
    # Note that we cannot guarantee an upper-bound of how long we will wait to do the task, only a lower bound
    # -- namely, delay_ms. That is, actual_task_execution_time >= created_at_ms + delay_ms, but in terms of
    # actual_task_execution_time <= created_at_ms + delay_ms + x, we cannot guarantee a maximum x.
    delay_ms: Optional[int]

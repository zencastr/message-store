import unittest
import unittest.mock as mock
from message_store.projections.fetch import Fetch, Projection
import asyncio
import json


class FetchTests(unittest.TestCase):
    def test_async_fetch_no_messages_returns_init(self):
        projection = mock.Mock()
        projection.get_result.return_value = {"result": "init"}
        fetch = TestableFetch()

        result = asyncio.run(fetch.fetch("some_subject.123", projection))

        self.assertEqual(result, {"result": "init"})
        fetch.ensure_consumer_is_deleted_mock.assert_called_once()

    def test_async_fetch_with_one_message_count_message_projection_returns_one(self):
        projection = Projection(
            init=lambda: {"count": 0},
            handlers={"TheEvent": lambda state, _: {"count": state["count"] + 1}},
        )
        fetch = TestableFetch(
            messages_to_return=[{"type": "TheEvent", "data": {}}],
        )

        result = asyncio.run(fetch.fetch("subject", projection))

        fetch.ensure_consumer_is_deleted_mock.assert_called_once()
        self.assertEqual(result, {"count": 1})

    def test_async_fetch_with_three_messages_count_message_projection_returns_three(
        self,
    ):
        projection = Projection(
            init=lambda: {"count": 0},
            handlers={"TheEvent": lambda state, _: {"count": state["count"] + 1}},
        )
        fetch = TestableFetch(
            messages_to_return=[
                {"type": "TheEvent", "data": {}},
                {"type": "TheEvent", "data": {}},
                {"type": "TheEvent", "data": {}},
                {"type": "UnrelatedEvent", "data": {}},
            ],
        )

        result = asyncio.run(fetch.fetch("some_subject.123", projection))

        fetch.ensure_consumer_is_deleted_mock.assert_called_once()
        self.assertEqual(result, {"count": 3})

    def test_async_fetch_with_three_messages_count_and_until_seq_2_message_projection_returns_two(
        self,
    ):
        projection = Projection(
            init=lambda: {"count": 0},
            handlers={"TheEvent": lambda state, _: {"count": state["count"] + 1}},
        )
        fetch = TestableFetch(
            messages_to_return=[
                {"type": "TheEvent", "data": {}},  # seq 1
                {"type": "TheEvent", "data": {}},  # seq 2
                {"type": "TheEvent", "data": {}},  # seq 3
                {"type": "UnrelatedEvent", "data": {}},  # seq 4
            ],
        )

        result = asyncio.run(fetch.fetch("some_subject.123", projection, until_seq=2))

        fetch.ensure_consumer_is_deleted_mock.assert_called_once()
        self.assertEqual(result, {"count": 2})

    def test_async_fetch_with_three_messages_count_and_until_seq_3_unrelated_message_affects_count_returns_two(
        self,
    ):
        projection = Projection(
            init=lambda: {"count": 0},
            handlers={"TheEvent": lambda state, _: {"count": state["count"] + 1}},
        )
        fetch = TestableFetch(
            messages_to_return=[
                {"type": "TheEvent", "data": {}},  # seq 1
                {"type": "UnrelatedEvent", "data": {}},  # seq 2
                {"type": "TheEvent", "data": {}},  # seq 3
                {"type": "TheEvent", "data": {}},  # seq 4
            ],
        )

        result = asyncio.run(fetch.fetch("some_subject.123", projection, until_seq=3))

        fetch.ensure_consumer_is_deleted_mock.assert_called_once()
        self.assertEqual(result, {"count": 2})


class TestableFetch(Fetch):
    def __init__(self, messages_to_return=[]):
        self.ensure_consumer_is_deleted_mock = mock.AsyncMock()
        self._ensure_consumer_is_deleted = self.ensure_consumer_is_deleted_mock
        self.jetstrean_client_mock = mock.Mock(
            subscribe=mock.AsyncMock(
                return_value=mock.AsyncMock(
                    messages=self._create_messages_iterator(
                        subject="some_subject_value_not_used",
                        messages=messages_to_return,
                    ),
                    consumer_info=mock.AsyncMock(
                        return_value=mock.Mock(
                            num_pending=len(messages_to_return), delivered=None
                        )
                    ),
                )
            )
        )
        super().__init__(self.jetstrean_client_mock, "the_prefix_doesnt_matter")

    async def _create_messages_iterator(self, subject, messages):
        for index, message in enumerate(messages):
            yield mock.Mock(
                subject=subject,
                data=json.dumps(message).encode(),
                timestamp=mock.Mock(),
                num_delivered=0,
                metadata=mock.Mock(sequence=mock.Mock(stream=index + 1)),
            )

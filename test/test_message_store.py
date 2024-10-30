"""
message-store test suite
assumes local nats instance (ideally fresh) available at port 4222
"""
import asyncio

import pytest
import pytest_asyncio

import nats
from nats.aio.client import Client

from message_store import MessageStore, Subscription, MessageFromSubscription, Message


@pytest_asyncio.fixture
async def nats_client():
    async with asyncio.Semaphore(1):
        client = await nats.connect("nats://127.0.0.1:4222")
        jetstream = client.jetstream()
        for stream_info in await jetstream.streams_info():
            await jetstream.delete_stream(stream_info.config.name)
        yield client
        await client.close()


@pytest.mark.asyncio
async def test_consumer_creation(nats_client: Client):
    message_store = MessageStore(nats_client, prefix="test", should_create_missing_streams=True)
    stream_suffix = "make-stream"
    await message_store.ensure_stream(stream_suffix)
    streams_info = await message_store._jetstream.streams_info()
    assert any([si.config.name.endswith(stream_suffix) for si in streams_info])


@pytest.mark.asyncio
async def test_consumer_creation_failure(nats_client: Client):
    message_store = MessageStore(nats_client, prefix="test", should_create_missing_streams=False)
    with pytest.raises(Exception):
        await message_store.ensure_stream("dont-make-stream")

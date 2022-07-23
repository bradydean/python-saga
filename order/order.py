import asyncio
import logging
import pydantic
import json

from uuid import UUID
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from contextlib import asynccontextmanager


logging.basicConfig()
logger = logging.getLogger("order")
logger.setLevel(logging.DEBUG)


class OrderCreate(pydantic.BaseModel):
    request_id: UUID
    id: UUID
    line_items: list[dict]
    type: str = "order.create"


class OrderCancel(pydantic.BaseModel):
    id: UUID
    request_id: UUID
    type: str = "order.cancel"


class OrderConfirm(pydantic.BaseModel):
    id: UUID
    request_id: UUID
    type: str = "order.confirm"


class OrderDeny(pydantic.BaseModel):
    id: UUID
    request_id: UUID
    type: str = "order.deny"


def parse_order_event(b: bytes):
    """Parse order event to pydantic model."""

    event = json.loads(b.decode("utf-8"))

    model = {
        "order.create": OrderCreate,
        "order.cancel": OrderCancel,
        "order.confirm": OrderConfirm,
        "order.deny": OrderDeny,
    }

    return model[event["type"]](**event)


@asynccontextmanager
async def kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers="kafka:9092",
        client_id="order",
        key_serializer=lambda u: str(u).encode("utf-8"),
        value_serializer=lambda o: o.json().encode("utf-8"),
    )

    await producer.start()

    try:
        yield producer
    finally:
        await producer.stop()


@asynccontextmanager
async def kafka_consumer():
    consumer = AIOKafkaConsumer(
        "orders",
        bootstrap_servers="kafka:9092",
        client_id="order",
        key_deserializer=lambda b: UUID(b.decode("utf-8")),
        value_deserializer=parse_order_event,
    )

    await consumer.start()

    try:
        yield consumer
    finally:
        await consumer.stop()


async def main():
    async with kafka_producer() as producer:
        async with kafka_consumer() as consumer:
            async for event in consumer:
                if event.value.type == "order.create":
                    logger.info(f"{event.value.request_id} received create order")
                    if len(event.value.line_items) == 0:
                        logger.info(f"{event.value.request_id} order deny")
                        confirmation = OrderDeny(
                            request_id=event.value.request_id,
                            id=event.value.id,
                        )
                    else:
                        logger.info(f"{event.value.request_id} order confirm")
                        confirmation = OrderConfirm(
                            request_id=event.value.request_id,
                            id=event.value.id,
                        )
                    await producer.send(
                        "orders", key=event.value.id, value=confirmation
                    )


if __name__ == "__main__":
    asyncio.run(main())

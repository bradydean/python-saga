import pydantic
import json
import asyncio
import logging

from fastapi import FastAPI, Response
from uuid import UUID, uuid4
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager


logging.basicConfig()
logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)

app = FastAPI()


class Order(pydantic.BaseModel):
    id: UUID
    line_items: list[dict]


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
        client_id="saga-web",
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
        "orders.reply",
        bootstrap_servers="kafka:9092",
        client_id="saga-web",
        key_deserializer=lambda b: UUID(b.decode("utf-8")),
        value_deserializer=parse_order_event,
    )

    await consumer.start()

    try:
        yield consumer
    finally:
        await consumer.stop()


@app.post("/orders")
async def create_order(order: Order, response: Response):
    """Create order using saga pattern."""

    request_id = uuid4()
    logger.info(f"{request_id} create order")

    async with kafka_producer() as producer:
        async with kafka_consumer() as consumer:

            # Send order.create event
            await producer.send(
                "orders",
                key=order.id,
                value=OrderCreate(
                    request_id=request_id,
                    id=order.id,
                    line_items=order.line_items,
                ),
            )

            async def wait_for_confirmation() -> str:
                """Consume events until order is confirmed or denied."""

                async for event in consumer:
                    if (
                        event.value.type == "order.confirm"
                        and event.value.id == order.id
                    ):
                        return "confirmed"
                    elif (
                        event.value.type == "order.deny" and event.value.id == order.id
                    ):
                        return "denied"

            try:
                # Wait 15 seconds for order to be confirmed
                status = await asyncio.wait_for(wait_for_confirmation(), 15)
            except asyncio.TimeoutError:
                status = "timeout"

            if status == "confirmed":
                response.status_code = 201
                return order
            elif status == "denied":
                return Response(status_code=422)
            elif status == "timeout":
                # Cancel order if not confirmed in time
                logger.info(f"{request_id} order canceled due to timeout")
                await producer.send(
                    "orders",
                    key=order.id,
                    value=OrderCancel(
                        id=order.id,
                        request_id=request_id,
                    ),
                )
                return Response(status_code=503)


@app.get("/")
async def hello_world():
    return "Hello world!"

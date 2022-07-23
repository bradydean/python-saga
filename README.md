# Saga demonstration with FastAPI and Kafka

The idea of the saga pattern is that when some service is processing some event X
(could be a kafka event or HTTP request) it must produce an event Y and wait for another
event Z in response. Processing of X cannot continue until Z is received.

The tricky part for HTTP is dealing with TCP timeout. The client must receive a response
within a reasonable amount of time. For asynchronous services this may not be a
problem, but even some asynchronous processes may need to impose a timeout due to external
constraints (eg process must complete hourly, daily, etc or else some other action must
be taken).

This repo includes an asynchronous order service. An order is created in the `orders` topic
and is then validated by the order service. A confirmation or denial is published.

Say we wanted a REST interface for this ordering service. The HTTP handler needs to
publish an order then wait for confirmation. If confirmed, return HTTP 201, or 422 if
denied. This handler must be aware of TCP timeout and return a response before the connection
is closed. However, it must be smart and cancel the order if timeout occurs so that
the order is not fulfilled while the client thinks the order was not created.

The FastAPI handler implemented here produces the order, then immediately starts consuming
events looking for a confirmation of the order id. If it is not received within 15 seconds,
the client is sent a HTTP 503 and the order is cancelled by producing another event.

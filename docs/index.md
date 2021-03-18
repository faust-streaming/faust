![faust-streaming](./images/banner-alt1.png)

# Faust Streaming

![python versions](https://img.shields.io/badge/python-3.6%203.7%203.8-blue)
![version](https://img.shields.io/badge/version-0.2.0-green)
[![codecov](https://codecov.io/gh/faust-streaming/faust/branch/master/graph/badge.svg?token=QJFBYNN0JJ)](https://codecov.io/gh/faust-streaming/faust)

## Installation

```bash
pip install faust-streaming
```

### Faust is for

- Event Processing
- Distributed Joins & Aggregations
- Machine Learning
- Asynchronous Tasks
- Distributed Computing
- Data Denormalization
- Intrusion Detection
- Realtime Web & Web Sockets

### What do I need

Faust requires Python 3.6 or later, and a running Kafka broker.

- Python 3.6 or later.
- Kafka 0.10.1 or later.

## What can it do

### Agents

Process infinite streams in a straightforward manner using
asynchronous generators. The concept of "agents" comes from
the actor model, and means the stream processor can execute
concurrently on many CPU cores, and on hundreds of machines
at the same time.

Use regular Python syntax to process streams and reuse your favorite
libraries:

```python
@app.agent()
async def process(stream):
    async for value in stream:
        process(value)
```

### Tables

Tables are sharded dictionaries that enable stream processors
to be stateful with persistent and durable data.

Streams are partitioned to keep relevant data close, and can be easily
repartitioned to achieve the topology you need.

In this example we repartition an order stream by account id, to count
orders in a distributed table:

```python
import faust

# this model describes how message values are serialized
# in the Kafka "orders" topic.
class Order(faust.Record, serializer='json'):
    account_id: str
    product_id: str
    amount: int
    price: float

app = faust.App('hello-app', broker='kafka://localhost')
orders_kafka_topic = app.topic('orders', value_type=Order)

# our table is sharded amongst worker instances, and replicated
# with standby copies to take over if one of the nodes fail.
order_count_by_account = app.Table('order_count', default=int)

@app.agent(orders_kafka_topic)
async def process(orders: faust.Stream[Order]) -> None:
    async for order in orders.group_by(Order.account_id):
        order_count_by_account[order.account_id] += 1
```

If we start multiple instances of this Faust application on many machines, any order with the same account id will be received by the same stream processing agent, so the count updates correctly in the table.

Sharding/partitioning is an essential part of stateful stream processing applications, so take this into account when designing your system, but note that streams can also be processed in round-robin order so you can use Faust for event processing and as a task queue also.

### Asynchronous

Faust takes full advantage of `asyncio` and the new :keyword:`async <async def>`/:keyword:`await` keywords in Python 3.6+ to run multiple stream processors in the same process, along with web servers and other network services.

## Faust key points

### Simple

Faust is extremely easy to use. To get started using other stream processing solutions you have complicated hello-world projects, and infrastructure requirements. Faust only requires Kafka, the rest is just Python, so If you know Python you can already use Faust to do stream processing, and it can integrate with just about anything.

Here's one of the easier applications you can make::

```python
import faust

class Greeting(faust.Record):
    from_name: str
    to_name: str

app = faust.App('hello-app', broker='kafka://localhost')
topic = app.topic('hello-topic', value_type=Greeting)

@app.agent(topic)
async def hello(greetings):
    async for greeting in greetings:
        print(f'Hello from {greeting.from_name} to {greeting.to_name}')

@app.timer(interval=1.0)
async def example_sender(app):
    await hello.send(
        value=Greeting(from_name='Faust', to_name='you'),
    )

if __name__ == '__main__':
    app.main()
```

You're probably a bit intimidated by the `async` and `await` keywords, but you don't have to know how `asyncio` works to use Faust: just mimic the examples, and you'll be fine.

The example application starts two tasks: one is processing a stream, the other is a background thread sending events to that stream. In a real-life application, your system will publish events to Kafka topics that your processors can consume from, and the background thread is only needed to feed data into our example.

### Highly Available

Faust is highly available and can survive network problems and server crashes.  In the case of node failure, it can automatically recover, and tables have standby nodes that will take over.

### Distributed

Start more instances of your application as needed.

### Fast

A single-core Faust worker instance can already process tens of thousands of events every second, and we are reasonably confident that throughput will increase once we can support a more optimized Kafka client.

### Flexible

Faust is just Python, and a stream is an infinite asynchronous iterator. If you know how to use Python, you already know how to use Faust, and it works with your favorite Python libraries like Django, Flask, SQLAlchemy, NTLK, NumPy, SciPy, TensorFlow, etc.

## Extensions

| Name     | Version | Bundle                                               |
|--------------|-----------|------------------------------------------------|
| `rocksdb`  | 5.0         | `pip install faust[rocksdb]`                   |
| `redis`    | aredis 1.1  | `pip install faust[redis]`                     |
| `datadog`  | 0.20.0      | `pip install faust[datadog]`                   |
| `statsd`   | 3.2.1       | `pip install faust[statsd]`                    |
| `uvloop`   | 0.8.1       | `pip install faust[uvloop]`                    |
| `eventlet` | 1.16.0      | `pip install faust[eventlet]`                  |
| `yaml`     | 5.1.0       | `pip install faust[yaml]`                      |

For pptimizations these can be all installed using `pip install faust[fast]`:

| Name         | Version | Bundle                                                   |
|------------------|-------------|--------------------------------------------------|
| `aiodns`       | 1.1.0       | `pip install faust[aiodns]`                        |
| `cchardet`     | 1.1.0       | `pip install faust[cchardet]`                      |
| `ciso8601`     | 2.1.0       | `pip install faust[ciso8601]`                      |
| `cython`       | 0.9.26      | `pip install faust[cython]`                        |
| `orjson`       | 2.0.0       | `pip install faust[orjson]`                        |
| `setproctitle` | 1.1.0       | `pip install faust[setproctitle]`                  |

For debugging extra these can be all installed using `pip install faust[debug]`:

| Name         | Version | Bundle                                                   |
|--------------|---------|----------------------------------------------------------|
| `aiomonitor` | 0.3         | `pip install faust[aiomonitor]`                      |
| `setproctitle` | 1.1.0       | `pip install faust[setproctitle]`                  |

To specify multiple extensions at the same time separate extensions with the comma:

```bash
pip install faust[uvloop,fast,rocksdb,datadog,redis]
```

To install :pypi:`python-rocksdb` on MacOS Sierra you need to specify some additional compiler flags:

```bash
CFLAGS='-std=c++11 -stdlib=libc++ -mmacosx-version-min=10.10' && pip install -U --no-cache python-rocksdb
```

## Design considerations

### Modern Python

Faust uses current Python 3 features such as :keyword:`async <async def>`/:keyword:`await` and type annotations. It's statically typed and verified by the [mypy](http://mypy-lang.org) type checker. You can take advantage of type annotations when writing Faust applications, but this is not mandatory.

### Library

Faust is designed to be used as a library, and embeds into any existing Python program, while also including helpers that make it easy to deploy applications without boilerplate.

### Supervised

The Faust worker is built up by many different services that start and stop in a certain order.  These services can be managed by supervisors, but if encountering an irrecoverable error such as not recovering from a lost Kafka connections, Faust is designed to crash.

For this reason Faust is designed to run inside a process supervisor tool such as [supervisord](http://supervisord.org), [Circus](http://circus.readthedocs.io/), or one provided by your Operating System.

### Extensible

Faust abstracts away storages, serializers, and even message transports, to make it easy for developers to extend Faust with new capabilities, and integrate into your existing systems.

### Lean

The source code is short and readable and serves as a good starting point for anyone who wants to learn how Kafka stream processing systems work.

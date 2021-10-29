#!/usr/bin/env python
import asyncio
import faust

WORDS = ['the', 'quick', 'brown', 'fox']


app = faust.App(
    'word-counts',
    broker='kafka://localhost:9092',
    store='rocksdb://',
    version=1,
    topic_partitions=8,
)

posts_topic = app.topic('posts', value_type=str)
word_counts = app.Table('word_counts', default=int,
                        help='Keep count of words (str to int).')


@app.agent(posts_topic)
async def shuffle_words(posts):
    async for post in posts:
        for word in post.split():
            await count_words.send(key=word, value=word)

last_count = {w:0 for w in WORDS}
@app.agent(value_type=str)
async def count_words(words):
    """Count words from blog post article body."""
    async for word in words:
        word_counts[word] += 1
        last_count[word] = word_counts[word]


@app.page('/count/{word}/')
@app.table_route(table=word_counts, match_info='word')
async def get_count(web, request, word):
    return web.json({
        word: word_counts[word],
    })

@app.page('/last/{word}/')
@app.topic_route(topic=posts_topic, match_info='word')
async def get_last(web, request, word):
    return web.json({
        word: last_count,
    })

@app.task
async def sender():
    await posts_topic.maybe_declare()

    for word in WORDS:
        for _ in range(1000):
            await shuffle_words.send(value=word)

    await asyncio.sleep(5.0)
    print(word_counts.as_ansitable(
        key='word',
        value='count',
        title='$$ TALLY $$',
        sort=True,
    ))


@app.on_rebalance_complete.connect
async def on_rebalance_complete(sender, **kwargs):
    print(word_counts.as_ansitable(
        key='word',
        value='count',
        title='$$ TALLY - after rebalance $$',
        sort=True,
    ))


if __name__ == '__main__':
    app.main()

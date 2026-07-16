from my_faust.app import faust_app
from my_faust.table.my_table import greetings_table
from my_faust.topic.my_topic import greetings_topic


@faust_app.agent(greetings_topic)
async def print_greetings(greetings):
    async for greeting in greetings:
        print(f"greeting: {greeting}")
        greetings_table[greeting] = {"hello world"}

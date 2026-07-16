from my_faust.app import faust_app

greetings_topic = faust_app.topic("greetings", value_type=str)

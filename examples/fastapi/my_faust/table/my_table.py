from my_faust.app import faust_app

greetings_table = faust_app.GlobalTable(
    name="greetings_table",
    default=dict,
    partitions=1,
    recovery_buffer_size=1,
)

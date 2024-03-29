import faust


def get_all_packages_to_scan():
    return ["my_faust"]


# You MUST have "app" defined in order for Faust to discover the app
# if you're using "faust" on CLI, but this doesn't work yet
# autodiscover https://faust-streaming.github.io/faust/userguide/settings.html#autodiscover
app = faust_app = faust.App(
    'hello-world-fastapi',
    broker='kafka://localhost:9092',
    web_enabled=False,
    autodiscover=get_all_packages_to_scan,
)

# For now, run via "faust -A my_faust.app worker -l info"

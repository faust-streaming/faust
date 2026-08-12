import faust


def get_all_packages_to_scan():
    return ["my_faust"]


# ``faust -A <module>`` looks for an attribute named ``app``, so bind that name
# to the Faust app -- not to the FastAPI object.  That is the whole trick.
# autodiscover:
# https://faust-streaming.github.io/faust/userguide/settings.html#autodiscover
app = faust_app = faust.App(
    "hello-world-fastapi",
    broker="kafka://localhost:9092",
    web_enabled=False,
    autodiscover=get_all_packages_to_scan,
)

# Run the worker with "faust -A my_faust.app worker -l info",
# or serve the API with "uvicorn main:api" (see ../main.py).

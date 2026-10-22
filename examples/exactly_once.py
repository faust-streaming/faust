"""Run the exactly-once Kafka crash-window validation."""

import os


def main() -> None:
    os.environ["SEMANTICS_MODE"] = "exactly-once"
    from examples.semantics_validation import main as run_validation

    run_validation()


if __name__ == "__main__":
    main()
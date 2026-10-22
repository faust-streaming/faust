"""Run the manual early-ack at-most-once Kafka validation."""

import os


def main() -> None:
    os.environ["SEMANTICS_MODE"] = "at-most-once"
    from examples.semantics_validation import main as run_validation

    run_validation()


if __name__ == "__main__":
    main()
"""Start an execution of the ncaa-d1-best-line workflow with the sample input.

Usage:
    export MISTRAL_API_KEY=...
    python examples/run_execution.py
"""
import json
import os
from pathlib import Path

from mistralai.client import Mistral


def main() -> None:
    client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
    sample = Path(__file__).parent / "sample_input.json"
    with sample.open() as f:
        payload = json.load(f)
    execution = client.workflows.execute_workflow(
        workflow_identifier="ncaa-d1-best-line",
        input=payload,
        wait_for_result=True,
    )
    print(execution.model_dump_json(indent=2))


if __name__ == "__main__":
    main()

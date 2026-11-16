"""Start an execution of the ncaa-d1-best-line workflow.

Usage:
    export MISTRAL_API_KEY=...
    python workflows/ncaa-d1-best-line/examples/run_execution.py                                                     # live ESPN FBS slate (sample_input.json)
    python workflows/ncaa-d1-best-line/examples/run_execution.py workflows/ncaa-d1-best-line/examples/sample_input_fixed.json  # fixed example slate
"""
import json
import os
import sys
from pathlib import Path

from mistralai.client import Mistral


def main() -> None:
    client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
    default_path = Path(__file__).parent / "sample_input.json"
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_path
    with path.open() as f:
        payload = json.load(f)
    print("Triggering ncaa-d1-best-line with input: " + str(path))
    execution = client.workflows.execute_workflow(
        workflow_identifier="ncaa-d1-best-line",
        input=payload,
        wait_for_result=True,
    )
    raw = execution.model_dump_json(indent=2)
    print(raw)
    try:
        data = json.loads(raw)
        output = data.get("output") if isinstance(data, dict) else None
        if isinstance(output, dict) and output.get("report_markdown"):
            print(chr(10) + "===== REPORT =====" + chr(10))
            print(output["report_markdown"])
    except Exception:
        pass


if __name__ == "__main__":
    main()

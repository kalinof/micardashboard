"""Execute all ESMA pipelines in sequence."""
from __future__ import annotations

import json

import requests

from .casps_pipeline import run as run_casps
from .non_compliant_pipeline import run as run_non_compliant


def main() -> None:
    session = requests.Session()
    session.trust_env = False

    results = {
        "casps": run_casps(session=session),
        "non_compliant": run_non_compliant(session=session),
    }
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()


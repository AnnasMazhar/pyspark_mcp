#!/usr/bin/env python3
"""
Lightweight healthcheck for Docker HEALTHCHECK.

This script attempts to import pyspark_tools.server and verify the
expected `app` attribute exists. Keep this small: it should surface
obvious import/config problems but avoid heavy initialization.

Exit code 0 == healthy, non-zero == unhealthy.
"""

import sys
import importlib

try:
    mod = importlib.import_module("pyspark_tools.server")

    # Basic validation: module has attribute "app" with a callable run or similar
    app = getattr(mod, "app", None)
    if app is None:
        print("UNHEALTHY: pyspark_tools.server.app not found", file=sys.stderr)
        sys.exit(1)

    # Optional: ensure app object has run attribute or similar
    if not hasattr(app, "run") and not callable(app):
        # not necessarily an error in all designs; warn but consider healthy
        print(
            "WARN: app object does not look like a server (no run attribute). Continuing healthcheck",
            file=sys.stderr,
        )

    print("OK")
    sys.exit(0)

except Exception as exc:
    print(
        f"UNHEALTHY: exception when importing pyspark_tools.server: {exc}",
        file=sys.stderr,
    )
    sys.exit(1)

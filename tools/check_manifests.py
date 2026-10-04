#!/usr/bin/env python3
"""Fail loudly on manifests that will be rejected at apply time.

Runs in CI and locally (`python tools/check_manifests.py`). PyYAML is the only extra
dependency, deliberately not part of the runtime install: this checks deployment
artifacts, which are a build-time concern.

Two things are verified. Every YAML file under deploy/ must parse and carry the fields
Kubernetes/Docker Compose require, and the manifests that `plexus.scaling.autoscaler`
renders must agree with the ones committed under deploy/k8s/base -- drift between a
renderer and a checked-in manifest means one of them is lying to whoever reads it.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - build-time dependency
    print(
        "check_manifests needs PyYAML: pip install -e '.[dev]'",
        file=sys.stderr,
    )
    raise SystemExit(2)

ROOT = Path(__file__).resolve().parents[1]
K8S_BASE = ROOT / "deploy" / "k8s" / "base"


def documents(path: Path) -> list[dict[str, Any]]:
    parsed = [doc for doc in yaml.safe_load_all(path.read_text()) if doc]
    if not parsed:
        raise ValueError("no documents")
    return [doc for doc in parsed if isinstance(doc, dict)]


def check_kubernetes(path: Path) -> int:
    objects = 0
    for doc in documents(path):
        for key in ("apiVersion", "kind"):
            if key not in doc:
                raise ValueError(f"missing {key!r}")
        objects += 1
    return objects


def check_compose(path: Path) -> int:
    doc = documents(path)[0]
    services = doc.get("services")
    if not isinstance(services, dict) or not services:
        raise ValueError("no services")
    for name, service in services.items():
        if not (service.get("image") or service.get("build")):
            raise ValueError(f"service {name!r} has neither image nor build")
    return len(services)


def main() -> int:
    failures = 0
    for path in sorted(ROOT.glob("deploy/**/*.yaml")) + sorted(ROOT.glob("deploy/**/*.yml")):
        try:
            if path.parent.name == "base" or path.parent.parent.name == "k8s":
                count = check_kubernetes(path)
                print(f"  ok  {path.relative_to(ROOT)} ({count} kubernetes object(s))")
            elif path.name.startswith("compose"):
                count = check_compose(path)
                print(f"  ok  {path.relative_to(ROOT)} ({count} service(s))")
            else:
                documents(path)
                print(f"  ok  {path.relative_to(ROOT)}")
        except Exception as exc:  # noqa: BLE001 - report every file, then fail once
            failures += 1
            print(f"  FAIL {path.relative_to(ROOT)}: {type(exc).__name__}: {exc}")

    try:
        from plexus.scaling.autoscaler import render_external_metric, render_hpa, render_keda
    except ImportError as exc:  # pragma: no cover - only when PYTHONPATH is unset
        print(f"  SKIP rendered manifests: {exc}")
    else:
        for label, document in (
            ("hpa", render_hpa()),
            ("keda", render_keda()),
            ("external-metric", render_external_metric()),
        ):
            try:
                kind = yaml.safe_load(document)["kind"]
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"  FAIL render_{label}: {type(exc).__name__}: {exc}")
                continue
            print(f"  ok  rendered {label} -> {kind}")

        shipped = (K8S_BASE / "scaledobject.yaml").read_text()
        rendered = render_keda()
        checks = {"query": "max(plexus_queue_depth)", "trigger": "type: prometheus"}
        for where, needle in checks.items():
            if needle not in shipped or needle not in rendered:
                failures += 1
                print(f"  FAIL {where}: {needle!r} missing from manifest or renderer")

    if failures:
        print(f"\n{failures} manifest problem(s)")
        return 1
    print("\nall manifests valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())

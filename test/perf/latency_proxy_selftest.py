#!/usr/bin/env python3
"""Dependency-free invariants for the bounded cellular TCP proxy."""

import ast
import pathlib
import types


ROOT = pathlib.Path(__file__).resolve().parents[2]
SOURCE = ROOT / "test" / "latency-proxy.py"


def load_module():
    # Avoid import machinery's bytecode cache so this works in read-only and
    # sandboxed test trees too.
    module = types.ModuleType("latency_proxy")
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
    exec(compile(tree, str(SOURCE), "exec"), module.__dict__)
    return module


def main() -> None:
    proxy = load_module()

    assert proxy._record("up", 3, 8)
    assert proxy._record("down", 5, 8)
    assert not proxy._record("down", 1, 8)
    snap = proxy._snapshot()
    assert snap["simulated"] is True
    assert snap["bytes_up"] == 3
    assert snap["bytes_down"] == 5
    assert snap["bytes_total"] == 8
    assert snap["budget_exceeded"] is True
    assert "target" not in snap and "listen" not in snap

    unlimited = proxy.Pacer(0)
    assert unlimited.bytes_per_sec == 0

    print("latency_proxy_selftest: PASS")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["z3-solver", "pyyaml"]
# ///
"""
S3Router config validator using Z3.
Usage: uv run scripts/validate_config.py router.yaml
"""

import sys
import yaml
from z3 import Solver, Int, Or, Not, Implies, sat

PRIMARY, SECONDARY, MIRROR, BEST_EFFORT, FALLBACK = 0, 1, 2, 3, 4
ACTION_MAP = {
    "primary": PRIMARY,
    "secondary": SECONDARY,
    "mirror": MIRROR,
    "best-effort": BEST_EFFORT,
    "fallback": FALLBACK,
}
DUAL_ACTIONS = {MIRROR, BEST_EFFORT, FALLBACK}
MULTIPART_OPS = [
    "CreateMultipartUpload",
    "UploadPart",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "ListParts",
]


def parse_yaml(path: str) -> list[dict]:
    """Parse router.yaml into flat rules list."""
    with open(path) as f:
        cfg = yaml.safe_load(f)
    rules = []
    for rule in cfg.get("rules", []):
        bucket = rule["bucket"]
        for prefix, actions in rule.get("prefix", {}).items():
            rules.append(
                {
                    "bucket": bucket,
                    "prefix": "" if prefix == "*" else prefix,
                    "actions": actions,
                }
            )
    return rules


def validate(rules: list[dict]) -> tuple[bool, list[str]]:
    solver = Solver()
    errors = []

    # Create Z3 variables for each (bucket, prefix, op) -> action
    z3_vars = {}
    for r in rules:
        for op, action in r["actions"].items():
            key = (r["bucket"], r["prefix"], op)
            z3_vars[key] = Int(f"{key}")
            solver.add(z3_vars[key] == ACTION_MAP[action])

    def get_action(bucket, prefix, op):
        return z3_vars.get(
            (bucket, prefix, op), z3_vars.get((bucket, prefix, "*"), Int("_"))
        )

    def is_dual(v):
        return Or(v == MIRROR, v == BEST_EFFORT, v == FALLBACK)

    def uses_sec(v):
        return Or(v == SECONDARY, is_dual(v))

    prefixes = {(r["bucket"], r["prefix"]) for r in rules}

    # CONSTRAINT 1: child uses secondary → parent must too (for listing)
    for b, p1 in prefixes:
        for _, p2 in [(b2, p2) for b2, p2 in prefixes if b2 == b and p2 != p1]:
            if p2.startswith(p1) or p1 == "":
                solver.add(
                    Implies(
                        uses_sec(get_action(b, p2, "ListObjectsV2")),
                        uses_sec(get_action(b, p1, "ListObjectsV2")),
                    )
                )

    # CONSTRAINT 2: multipart ops cannot use dual-send
    for b, p in prefixes:
        for op in MULTIPART_OPS:
            solver.add(Not(is_dual(get_action(b, p, op))))

    if solver.check() == sat:
        return True, []

    # Generate error messages
    def resolve(bucket, prefix, op):
        for r in rules:
            if r["bucket"] == bucket and r["prefix"] == prefix:
                return r["actions"].get(op, r["actions"].get("*", "primary"))
        return "primary"

    for b, p1 in prefixes:
        for _, p2 in [(b2, p2) for b2, p2 in prefixes if b2 == b and p2 != p1]:
            if p2.startswith(p1) or p1 == "":
                child = resolve(b, p2, "ListObjectsV2")
                parent = resolve(b, p1, "ListObjectsV2")
                if (
                    child in {"secondary", "mirror", "best-effort", "fallback"}
                    and parent == "primary"
                ):
                    errors.append(
                        f"[{b}] prefix '{p2}' uses secondary, but parent '{p1 or '*'}' doesn't"
                    )

    for r in rules:
        default = r["actions"].get("*", "primary")
        for op in MULTIPART_OPS:
            action = r["actions"].get(op, default)
            if action in {"mirror", "best-effort", "fallback"}:
                errors.append(
                    f"[{r['bucket']}/{r['prefix'] or '*'}] {op}={action} forbidden"
                )

    return False, errors


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: uv run scripts/validate_config.py <router.yaml>")
        sys.exit(1)

    rules = parse_yaml(sys.argv[1])
    ok, errors = validate(rules)

    print(f"Validating: {sys.argv[1]}")
    print(f"Result: {'✓ VALID' if ok else '✗ INVALID'}")
    for e in errors:
        print(f"  - {e}")
    sys.exit(0 if ok else 1)

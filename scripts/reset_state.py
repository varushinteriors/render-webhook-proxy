#!/usr/bin/env python3
"""Utility to wipe stored conversation + meeting state after test runs."""
import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple

DEFAULT_TARGETS: Dict[str, Dict[str, Any]] = {
    "conversations": {
        "env": "STATE_PATH",
        "default": "logs/conversations.json",
        "empty": {},
        "label": "Conversation state",
    },
    "meetings": {
        "env": "MEETINGS_PATH",
        "default": "logs/meetings.json",
        "empty": [],
        "label": "Scheduled meetings",
    },
    "lead_engagement": {
        "env": "LEAD_ENGAGEMENT_PATH",
        "default": "logs/lead-engagement.json",
        "empty": {},
        "label": "Lead engagement tracker",
    },
    "lead_index": {
        "env": "LEAD_INDEX_PATH",
        "default": "logs/lead-index.json",
        "empty": {},
        "label": "Lead index cache",
    },
    "lead_scores": {
        "env": "LEAD_SCORE_PATH",
        "default": "logs/lead-scores.json",
        "empty": {},
        "label": "Lead scoring cache",
    },
}

DEFAULT_TEST_WA_IDS = [
    wa.strip()
    for wa in os.getenv(
        "TEST_WA_IDS",
        "919873607248,918570073000,918814000400",
    ).split(",")
    if wa.strip()
]


def _resolve_path(kind: str) -> Path:
    config = DEFAULT_TARGETS[kind]
    raw = os.getenv(config["env"], config["default"])
    return Path(raw)


def _reset_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    tmp.replace(path)


def _reset_conversations(path: Path, keep_wa_ids: Tuple[str, ...]) -> None:
    keep = tuple(wa for wa in keep_wa_ids if wa)
    if not keep:
        _reset_file(path, {})
        print("Cleared conversations →", path)
        return
    preserved = {}
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError:
            data = {}
        if isinstance(data, dict):
            preserved = {wa: data.get(wa) for wa in keep if wa in data}
    _reset_file(path, preserved)
    print(f"Cleared conversations → {path} (kept {len(preserved)} WA IDs)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset stored state files after testing.")
    parser.add_argument(
        "--target",
        dest="targets",
        choices=DEFAULT_TARGETS.keys(),
        action="append",
        help="Specific store(s) to wipe. Defaults to all.",
    )
    parser.add_argument("--keep-wa", dest="keep_wa", action="append", help="WA IDs to preserve in conversation state.")
    parser.add_argument("--include-tests", action="store_true", help="Also wipe default tester WA IDs.")
    parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt.")
    args = parser.parse_args()

    targets = args.targets or list(DEFAULT_TARGETS.keys())
    keep_wa = []
    if not args.include_tests:
        keep_wa.extend(DEFAULT_TEST_WA_IDS)
    if args.keep_wa:
        keep_wa.extend(args.keep_wa)
    keep_wa_tuple = tuple({wa.strip(): None for wa in keep_wa if wa}.keys())

    if keep_wa_tuple:
        print("Preserving WA IDs:", ", ".join(keep_wa_tuple))

    print("State reset targets:")
    for key in targets:
        config = DEFAULT_TARGETS[key]
        print(f" - {key}: {_resolve_path(key)} ({config['label']})")

    if not args.yes:
        choice = input("Type 'wipe' to confirm deletion: ").strip().lower()
        if choice != "wipe":
            print("Aborted. No files were touched.")
            return

    for key in targets:
        path = _resolve_path(key)
        if key == "conversations":
            _reset_conversations(path, keep_wa_tuple)
            continue
        payload = DEFAULT_TARGETS[key]["empty"]
        _reset_file(path, payload)
        print(f"Cleared {key} → {path}")

    print("Done. Upload the cleaned files to R2 if you mirror state remotely.")


if __name__ == "__main__":
    main()

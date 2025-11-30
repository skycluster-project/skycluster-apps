#!/usr/bin/env python3
"""
ubuntu_label_mapper.py

Map generic Ubuntu labels like "ubuntu-24.04" to hyperscaler-specific
image naming conventions, e.g.:

AWS   -> ubuntu-<codename>-<YY.MM>      (e.g., ubuntu-jammy-22.04)
GCP   -> ubuntu-<YYMM>                  (e.g., ubuntu-2404)
Azure -> <YY_MM>                        (e.g., 24_04)

This script now accepts labels with an optional "-gpu" suffix, e.g.:
  ubuntu-24.04-gpu
and will strip the suffix before producing mappings.
"""

from __future__ import annotations
import argparse
import json
import re
from typing import Dict, List, Optional

# --- Canonical Ubuntu codename map (extend as needed) ---
# Keys are "YY.MM" strings as they appear in labels like "ubuntu-22.04"
UBUNTU_CODENAMES: Dict[str, str] = {
  "24.10": "oracular",  # non-LTS
  "24.04": "noble",     # LTS
  "23.10": "mantic",
  "23.04": "lunar",
  "22.10": "kinetic",
  "22.04": "jammy",     # LTS
  "21.10": "impish",
  "21.04": "hirsute",
  "20.10": "groovy",
  "20.04": "focal",     # LTS
}

# --- Hyperscaler formatters (keep these small/safe-by-default) ---
def fmt_aws(version: str, codename: str) -> str:
  # Matches the user's example: "ubuntu-jammy-22.04"
  return f"ubuntu-{codename}-{version}"

def fmt_gcp(version: str, codename: str) -> str:
  # Matches the user's example: "ubuntu-2404"
  # (Note: Actual GCP families often add "-lts" for LTS; we follow the user's given pattern.)
  yymm = version.replace(".", "")
  return f"ubuntu-{yymm}"

def fmt_azure(version: str, codename: str) -> str:
  # Matches the user's example: "24_04"
  return version.replace(".", "_")

FORMATTERS = {
  "aws": fmt_aws,
  "gcp": fmt_gcp,
  "azure": fmt_azure,
}

# --- Core logic ---
# Accept optional "-gpu" suffix (but treat it as not part of the version string)
_LABEL_RE = re.compile(r"^ubuntu-(\d{2}\.\d{2})(?:-gpu)?$")

def _parse_label(label: str) -> Optional[str]:
  """
  Given a label like 'ubuntu-22.04' or 'ubuntu-22.04-gpu', return '22.04'.
  Return None if invalid.
  """
  m = _LABEL_RE.match(label.strip().lower())
  return m.group(1) if m else None

def map_label(label: str) -> Dict[str, str]:
  """
  Map a single generic label (e.g., 'ubuntu-22.04' or 'ubuntu-22.04-gpu')
  to provider-specific names.
  Returns a dict like: {"aws": "...", "gcp": "...", "azure": "..."}.

  Raises ValueError if the label is invalid or unknown.
  """
  version = _parse_label(label)
  if not version:
    raise ValueError(f"Invalid label format: '{label}'. Expected 'ubuntu-YY.MM' or 'ubuntu-YY.MM-gpu'.")

  codename = UBUNTU_CODENAMES.get(version)
  if not codename:
    raise ValueError(
      f"Unknown Ubuntu version '{version}'. Add it to UBUNTU_CODENAMES."
    )

  return {
    provider: fmt(version, codename)
    for provider, fmt in FORMATTERS.items()
  }

def map_labels(labels: List[str]) -> Dict[str, Dict[str, str]]:
  """
  Map multiple labels at once. Returns:
  {
    "ubuntu-22.04": {"aws": "...", "gcp": "...", "azure": "..."},
    ...
  }
  """
  output = {}
  for label in labels:
    output[label] = map_label(label)
  return output

# --- CLI ---
def _default_extended_list() -> List[str]:
  # Provide 10+ useful entries (LTS + some interims)
  return [
    "ubuntu-24.10",
    "ubuntu-24.04",
    "ubuntu-23.10",
    "ubuntu-23.04",
    "ubuntu-22.10",
    "ubuntu-22.04",
    "ubuntu-21.10",
    "ubuntu-21.04",
    "ubuntu-20.10",
    "ubuntu-20.04",
    "ubuntu-19.10",
    "ubuntu-19.04",
    "ubuntu-18.04",
  ]

def main():
  p = argparse.ArgumentParser(description="Map ubuntu-YY.MM labels to hyperscaler names.")
  p.add_argument("label", nargs="?", help="Label like 'ubuntu-22.04' or 'ubuntu-22.04-gpu'")
  p.add_argument("--provider", choices=sorted(FORMATTERS.keys()),
           help="Return only one provider's mapping.")
  p.add_argument("--json", action="store_true",
           help="Output JSON (default is pretty table).")
  args = p.parse_args()

  if not args.label:
    p.error("Provide a label like 'ubuntu-22.04'")

  try:
    mapping = map_label(args.label)
  except ValueError as e:
    raise SystemExit(str(e))

  if args.provider:
    print(mapping[args.provider])
  else:
    if args.json:
      print(json.dumps(mapping, indent=2, sort_keys=True))
    else:
      _print_table({args.label: mapping})

def _print_table(data: Dict[str, Dict[str, str]]) -> None:
  # Simple pretty-printer without external deps.
  labels = list(data.keys())
  col_names = ["label", "aws", "gcp", "azure"]
  rows = []
  for label in labels:
    rows.append([
      label,
      data[label]["aws"],
      data[label]["gcp"],
      data[label]["azure"],
    ])

  widths = [max(len(str(x)) for x in col) for col in zip(col_names, *rows)]
  def fmt_row(row): return " | ".join(s.ljust(w) for s, w in zip(row, widths))

  print(fmt_row(col_names))
  print("-+-".join("-" * w for w in widths))
  for r in rows:
    print(fmt_row([str(c) for c in r]))

# --- Example: compose into CLI/other tools to derive paths ---
def to_image_path(provider: str, label: str, base: str = "/images") -> str:
  """
  Build a simple path or identifier using the mapped value.
  Example:
    to_image_path("aws", "ubuntu-22.04") -> "/images/aws/ubuntu-jammy-22.04"
  """
  provider = provider.lower()
  if provider not in FORMATTERS:
    raise ValueError(f"Unknown provider: {provider}")
  return f"{base}/{provider}/{map_label(label)[provider]}"

if __name__ == "__main__":
  main()
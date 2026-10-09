#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
#
# Installs the waged-sim skill for GitHub Copilot CLI (personal skills folder) and records where the
# Helix repository is, so the skill's launcher can find and build the tool.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
TARGET="${1:-$HOME/.copilot/skills/waged-sim}"
mkdir -p "$TARGET/scripts"
cp "$HERE/SKILL.md" "$TARGET/SKILL.md"
cp "$HERE/scripts/waged-sim" "$HERE/scripts/render_xlsx.py" "$TARGET/scripts/"
chmod +x "$TARGET/scripts/waged-sim"
echo "$REPO" > "$TARGET/repo-path"
echo "Installed the waged-sim skill to $TARGET (Helix repo: $REPO)."
echo "Restart Copilot CLI or run /skills to pick it up."

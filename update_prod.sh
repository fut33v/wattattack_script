#!/usr/bin/env bash
# Update remote deployment on msk2 and restart stack.

set -euo pipefail

ssh root@msk2.fut33v.ru <<'SSH'
cd /root/wattattack_script
git pull
./start.sh
SSH

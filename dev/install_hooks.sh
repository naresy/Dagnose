#!/usr/bin/env bash
# Install a git pre-commit hook that blocks commits containing a real-looking key.
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
hook="$root/.git/hooks/pre-commit"
cat > "$hook" <<'HOOK'
#!/usr/bin/env bash
# Installed by dev/install_hooks.sh: refuse commits that contain a real-looking key.
exec uv run --quiet python dev/scan_secrets.py --staged
HOOK
chmod +x "$hook"
echo "installed $hook"

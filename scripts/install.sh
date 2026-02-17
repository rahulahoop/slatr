#!/usr/bin/env bash
#
# Install slatr from source.
#
# Builds the fat JAR via sbt and installs a `slatr` wrapper script
# so you can run `slatr <command>` from anywhere.
#
# Usage:
#   ./scripts/install.sh                             # default: /usr/local/bin
#   INSTALL_DIR=~/.local/bin ./scripts/install.sh    # custom location
#
set -euo pipefail

INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
JAR_DIR="${HOME}/.local/share/slatr"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_SOURCE="${PROJECT_DIR}/modules/cli/target/scala-2.13/slatr.jar"

# ── Build ──────────────────────────────────────────────────────
echo "==> Building slatr JAR..."
(cd "$PROJECT_DIR" && sbt -batch cli/assembly)

if [ ! -f "$JAR_SOURCE" ]; then
  echo "ERROR: Assembly JAR not found at $JAR_SOURCE"
  exit 1
fi

# ── Install JAR ────────────────────────────────────────────────
echo "==> Installing JAR to ${JAR_DIR}..."
mkdir -p "$JAR_DIR"
cp "$JAR_SOURCE" "$JAR_DIR/slatr.jar"

# ── Install wrapper script ─────────────────────────────────────

# Resolve sudo needs: if the directory isn't writable, try sudo.
# If sudo also fails (no tty / no password), fall back to ~/.local/bin.
SUDO=""
if [ ! -w "$INSTALL_DIR" ]; then
  if sudo -n true 2>/dev/null; then
    SUDO="sudo"
  else
    echo "    Cannot write to ${INSTALL_DIR} and sudo requires a password."
    INSTALL_DIR="${HOME}/.local/bin"
    echo "    Falling back to ${INSTALL_DIR}"
    mkdir -p "$INSTALL_DIR"
  fi
fi

WRAPPER="${INSTALL_DIR}/slatr"
echo "==> Creating wrapper at ${WRAPPER}..."

$SUDO mkdir -p "$INSTALL_DIR"

$SUDO tee "$WRAPPER" > /dev/null <<EOF
#!/usr/bin/env bash
exec java -jar "${JAR_DIR}/slatr.jar" "\$@"
EOF

$SUDO chmod +x "$WRAPPER"

# ── Verify PATH ────────────────────────────────────────────────
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
  echo ""
  echo "NOTE: ${INSTALL_DIR} is not on your PATH."

  # If fish is installed, add to fish PATH automatically
  if command -v fish > /dev/null 2>&1; then
    echo "Detected fish shell. Adding ${INSTALL_DIR} to fish PATH..."
    fish -c "fish_add_path ${INSTALL_DIR}"
  fi

  # Also print instructions for bash/zsh in case they use those too
  echo "For bash/zsh, add to your shell profile (~/.zshrc, ~/.bashrc):"
  echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

echo ""
echo "Done! You can now run:"
echo "  slatr --help"

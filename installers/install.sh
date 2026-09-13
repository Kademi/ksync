#!/bin/sh
# ksync3 installer for macOS, Linux and WSL. Needs curl or wget.
#   curl -fsSL https://raw.githubusercontent.com/Kademi/ksync/master/installers/install.sh | bash
# Installs ksync3.jar, a JRE if no Java 11+ is found, a `ksync3` launcher, and tab completion.
# Run as a normal user for a per-user install, or with sudo for /usr/local.
# Overrides: KSYNC3_HOME, KSYNC3_BIN, KSYNC3_VERSION, KSYNC3_JAR_URL, KSYNC3_JAVA_VERSION, KSYNC3_BUNDLE_JRE=1
set -eu

# The latest release, or KSYNC3_VERSION with or without its leading v, as in 1.8.9 or v1.8.9.
VERSION=${KSYNC3_VERSION:-latest}
case "$VERSION" in
  latest) BASE_URL=https://github.com/Kademi/ksync/releases/latest/download ;;
  v*)     BASE_URL=https://github.com/Kademi/ksync/releases/download/$VERSION ;;
  *)      BASE_URL=https://github.com/Kademi/ksync/releases/download/v$VERSION ;;
esac
JAR_URL=${KSYNC3_JAR_URL:-$BASE_URL/ksync3.jar}
JAVA_MIN=11
JAVA_VERSION=${KSYNC3_JAVA_VERSION:-25}

case "$(uname -s)" in
  Linux)  OS=linux ;;
  Darwin) OS=mac ;;
  MINGW*|MSYS*|CYGWIN*) echo "Git Bash is not supported. Use installers/install.ps1 or install.cmd on Windows." >&2; exit 1 ;;
  *) echo "Unsupported OS: $(uname -s). Use installers/install.ps1 on Windows." >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64)  ARCH=x64 ;;
  arm64|aarch64) ARCH=aarch64 ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac
# An x64 shell under Rosetta on an ARM Mac should still get the native JRE
if [ "$OS" = mac ] && [ "$ARCH" = x64 ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = 1 ]; then ARCH=aarch64; fi

# Where things go. Root installs system-wide, otherwise the platform's per-user locations:
# XDG data dir on Linux, Application Support on macOS, with the launcher in ~/.local/bin.
if [ "$(id -u)" -eq 0 ]; then
  HOME_DIR=${KSYNC3_HOME:-/usr/local/lib/ksync3}
  BIN_DIR=${KSYNC3_BIN:-/usr/local/bin}
elif [ "$OS" = mac ]; then
  HOME_DIR=${KSYNC3_HOME:-$HOME/Library/Application Support/ksync3}
  BIN_DIR=${KSYNC3_BIN:-$HOME/.local/bin}
else
  HOME_DIR=${KSYNC3_HOME:-${XDG_DATA_HOME:-$HOME/.local/share}/ksync3}
  BIN_DIR=${KSYNC3_BIN:-$HOME/.local/bin}
fi

if command -v curl >/dev/null 2>&1; then fetch() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then fetch() { wget -q -O "$2" "$1"; }
else echo "curl or wget is required" >&2; exit 1; fi

# The checksum comes from the same release as the file, so it catches a truncated or
# swapped download, not a compromised release. Exits rather than installing something
# that does not match what the release says it is. $SUMS empty means there is nothing
# to check against, which is the case for a KSYNC3_JAR_URL of your own.
verify_sha256() {
  [ -n "$SUMS" ] || return 0
  expected=$(awk -v name="$2" '$2 == name { print $1 }' "$SUMS")
  [ -n "$expected" ] || { echo "SHA256SUMS in the release has no line for $2" >&2; exit 1; }
  if command -v sha256sum >/dev/null 2>&1; then actual=$(sha256sum "$1" | cut -d' ' -f1)
  elif command -v shasum >/dev/null 2>&1; then actual=$(shasum -a 256 "$1" | cut -d' ' -f1)
  else echo "Neither sha256sum nor shasum is available, skipping the checksum check" >&2; return 0; fi
  if [ "$actual" != "$expected" ]; then
    rm -f "$1"
    echo "Checksum mismatch for $2" >&2
    echo "  expected $expected" >&2
    echo "  got      $actual" >&2
    exit 1
  fi
}

# Prints the major version of the java binary given, or nothing if it does not run.
java_major() {
  "$1" -version 2>&1 | sed -nE '1s/.*version "([0-9]+)(\.([0-9]+))?.*/\1 \3/p' | awk '{ print ($1 == 1) ? $2 : $1 }'
}

find_java() {
  for j in "${JAVA_HOME:-/nonexistent}/bin/java" "$(command -v java || true)"; do
    [ -x "$j" ] || continue
    case "$j" in /mnt/*) continue ;; esac  # WSL: a Windows java.exe on the interop PATH cannot take Linux paths
    v=$(java_major "$j")
    if [ -n "$v" ] && [ "$v" -ge "$JAVA_MIN" ]; then echo "$j"; return; fi
  done
}

mkdir -p "$HOME_DIR" "$BIN_DIR"

SUMS=""
if [ -z "${KSYNC3_JAR_URL:-}" ]; then
  SUMS=$HOME_DIR/SHA256SUMS.tmp
  fetch "$BASE_URL/SHA256SUMS" "$SUMS"
fi

echo "Downloading ksync3.jar ..."
fetch "$JAR_URL" "$HOME_DIR/ksync3.jar.tmp"
verify_sha256 "$HOME_DIR/ksync3.jar.tmp" ksync3.jar
mv "$HOME_DIR/ksync3.jar.tmp" "$HOME_DIR/ksync3.jar"

# Tab completion, from the release beside the jar it was generated from. picocli
# generates bash, and the script sets zsh up itself when sourced there.
COMPLETION=""
case "${SHELL##*/}" in
  bash|zsh)
    if [ -n "$SUMS" ]; then
      fetch "$BASE_URL/ksync3_completion" "$HOME_DIR/ksync3_completion.tmp"
      verify_sha256 "$HOME_DIR/ksync3_completion.tmp" ksync3_completion
      mv "$HOME_DIR/ksync3_completion.tmp" "$HOME_DIR/ksync3_completion"
      COMPLETION=$HOME_DIR/ksync3_completion
    fi
    ;;
esac
if [ -n "$SUMS" ]; then
  echo "Checksums verified"
  rm -f "$SUMS"
fi

JAVA=$( [ "${KSYNC3_BUNDLE_JRE:-}" = 1 ] || find_java )
if [ -n "$JAVA" ]; then
  echo "Using Java $(java_major "$JAVA") at $JAVA"
else
  echo "No Java $JAVA_MIN+ found, downloading Temurin JRE $JAVA_VERSION ($OS/$ARCH) ..."
  JRE_URL="https://api.adoptium.net/v3/binary/latest/$JAVA_VERSION/ga/$OS/$ARCH/jre/hotspot/normal/eclipse"
  fetch "$JRE_URL" "$HOME_DIR/jre.tar.gz"
  rm -rf "$HOME_DIR/jre"
  mkdir "$HOME_DIR/jre"
  tar -xzf "$HOME_DIR/jre.tar.gz" -C "$HOME_DIR/jre" --strip-components=1
  rm "$HOME_DIR/jre.tar.gz"
  JAVA="$HOME_DIR/jre/bin/java"
  [ "$OS" = mac ] && JAVA="$HOME_DIR/jre/Contents/Home/bin/java"
fi

cat > "$BIN_DIR/ksync3" <<LAUNCHER
#!/bin/sh
# ksync3 launcher, written by install.sh
[ -t 1 ] && printf '\\033]0;Ksync3 | %s\\007' "\$(basename "\$PWD")"
exec "$JAVA" -jar "$HOME_DIR/ksync3.jar" "\$@"
LAUNCHER
chmod +x "$BIN_DIR/ksync3"

echo "Installed ksync3 to $BIN_DIR/ksync3"

# The login shell's startup file, for the PATH and completion lines below
case "${SHELL##*/}" in
  zsh)  PROFILE=$HOME/.zshrc ;;
  fish) PROFILE=$HOME/.config/fish/config.fish ;;
  bash) PROFILE=$HOME/.bashrc ;;
  *)    PROFILE=$HOME/.profile ;;
esac

# Appends a line to the profile once, so running the installer again does not repeat it
add_line() {
  grep -qsF "$1" "$PROFILE" || printf '\n# ksync3\n%s\n' "$1" >> "$PROFILE"
}

mkdir -p "$(dirname "$PROFILE")"
case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *)
    # Put BIN_DIR on the PATH for the login shell. Ubuntu's default .profile does this itself
    # once ~/.local/bin exists, but macOS and most other distros do not.
    if [ "${SHELL##*/}" = fish ]; then
      add_line "fish_add_path $BIN_DIR"
    else
      add_line "export PATH=\"$BIN_DIR:\$PATH\""
    fi
    echo "Added $BIN_DIR to your PATH in $PROFILE. Open a new terminal to pick it up."
    ;;
esac

if [ -n "$COMPLETION" ]; then
  # Guarded, so removing the install directory leaves a working shell behind
  add_line "[ -f \"$COMPLETION\" ] && . \"$COMPLETION\""
  echo "Added tab completion in $PROFILE"
elif [ "${SHELL##*/}" != bash ] && [ "${SHELL##*/}" != zsh ]; then
  echo "No tab completion for ${SHELL##*/}, picocli generates it for bash and zsh only"
fi
echo "Uninstall: rm -rf \"$HOME_DIR\" \"$BIN_DIR/ksync3\", and the ksync3 lines in $PROFILE"

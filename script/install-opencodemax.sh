#!/usr/bin/env sh
set -eu

REPO="${OPENCODEMAX_REPO:-patrick-toulme/opencode}"
DEFAULT_TAG="__OPENCODEMAX_TAG__"
PLACEHOLDER_TAG="__OPENCODEMAX""_TAG__"
TAG="${OPENCODEMAX_TAG:-$DEFAULT_TAG}"
INSTALL_DIR="${OPENCODEMAX_INSTALL_DIR:-${XDG_BIN_DIR:-$HOME/.local/bin}}"
BASE_URL="${OPENCODEMAX_BASE_URL:-https://github.com/$REPO/releases/download/$TAG}"

if [ "$TAG" = "$PLACEHOLDER_TAG" ] || [ -z "$TAG" ]; then
  echo "OpenCodeMAX installer is missing a release tag." >&2
  echo "Set OPENCODEMAX_TAG=opencodemax-v<version> or download install.sh from a GitHub Release." >&2
  exit 1
fi

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "OpenCodeMAX installer requires '$1'." >&2
    exit 1
  fi
}

os_name="$(uname -s 2>/dev/null || echo unknown)"
arch_name="$(uname -m 2>/dev/null || echo unknown)"

case "$os_name" in
  Darwin) os="darwin" ;;
  Linux) os="linux" ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT) os="windows" ;;
  *)
    echo "Unsupported OS: $os_name" >&2
    exit 1
    ;;
esac

case "$arch_name" in
  arm64|aarch64) arch="arm64" ;;
  x86_64|amd64) arch="x64" ;;
  *)
    echo "Unsupported CPU architecture: $arch_name" >&2
    exit 1
    ;;
esac

target="$os-$arch"

if [ "${OPENCODEMAX_BASELINE:-0}" = "1" ]; then
  if [ "$arch" != "x64" ]; then
    echo "OPENCODEMAX_BASELINE=1 is only available for x64 builds." >&2
    exit 1
  fi
  target="$target-baseline"
fi

if [ "$os" = "linux" ]; then
  libc="glibc"
  if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
    libc="musl"
  elif [ -e /lib/ld-musl-x86_64.so.1 ] || [ -e /lib/ld-musl-aarch64.so.1 ]; then
    libc="musl"
  fi

  if [ "$libc" = "musl" ]; then
    target="$target-musl"
  fi
  asset="OpenCodeMAX-$target.tar.gz"
else
  asset="OpenCodeMAX-$target.zip"
fi

url="$BASE_URL/$asset"
tmp="${TMPDIR:-/tmp}/opencodemax-install.$$"
mkdir -p "$tmp"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

archive="$tmp/$asset"
echo "Downloading OpenCodeMAX $TAG for $target..."

if command -v curl >/dev/null 2>&1; then
  curl -fL "$url" -o "$archive"
elif command -v wget >/dev/null 2>&1; then
  wget -O "$archive" "$url"
else
  echo "OpenCodeMAX installer requires curl or wget." >&2
  exit 1
fi

case "$asset" in
  *.tar.gz)
    require tar
    tar -xzf "$archive" -C "$tmp"
    ;;
  *.zip)
    require unzip
    unzip -q "$archive" -d "$tmp"
    ;;
esac

binary="$tmp/opencode"
if [ ! -f "$binary" ] && [ -f "$tmp/opencode.exe" ]; then
  binary="$tmp/opencode.exe"
fi

if [ ! -f "$binary" ]; then
  echo "Downloaded archive did not contain an opencode binary." >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
dest="$INSTALL_DIR/opencode"
if [ "$os" = "windows" ]; then
  dest="$INSTALL_DIR/opencode.exe"
fi
cp "$binary" "$dest"
chmod +x "$dest"

echo "Installed OpenCodeMAX to $dest"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "Add $INSTALL_DIR to PATH if 'opencode' is not found."
    ;;
esac

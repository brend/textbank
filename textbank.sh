#!/usr/bin/env bash
# TextBank CLI wrapper for grpcurl (macOS)
# - Intern: (lang, text) -> id  (auto base64-encodes)
# - Get:    (id, lang) -> text  (auto decodes)
# Requires: grpcurl, base64
set -euo pipefail

# --- Config ---
TARGET="${TEXTBANK_TARGET:-127.0.0.1:50051}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="${TEXTBANK_PROTO_DIR:-$SCRIPT_DIR/proto}"
PROTO_FILE="${TEXTBANK_PROTO_FILE:-text_bank.proto}"
SERVICE="textbank.TextBank"

GRPCURL_BIN="${GRPCURL_BIN:-grpcurl}"

# --- Helpers ---
die() { echo "Error: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "Missing dependency: $1"; }

check_env() {
  need "$GRPCURL_BIN"
  need base64
  [[ -f "$PROTO_DIR/$PROTO_FILE" ]] || die "Proto not found: $PROTO_DIR/$PROTO_FILE
Set TEXTBANK_PROTO_DIR/TEXTBANK_PROTO_FILE if your proto lives elsewhere."
}

b64enc() {
  # encode without line breaks
  printf '%s' "$1" | base64 | tr -d '\n'
}

b64dec() {
  printf '%s' "$1" | base64 -D 2>/dev/null || printf '%s' "$1"
}

grpc_intern() {
  local lang="$1"; shift
  local text="$*"
  [[ -n "$lang" ]] || die "lang is required"
  local b64; b64="$(b64enc "$text")"
  "$GRPCURL_BIN" -plaintext \
    -import-path "$PROTO_DIR" -proto "$PROTO_FILE" \
    -d "{\"lang\":\"$lang\",\"text\":\"$b64\"}" \
    "$TARGET" "$SERVICE/Intern"
}

grpc_get() {
  local id="$1"; local lang="$2"; local mode="${3:-text}"  # mode: text|b64|json
  [[ -n "$id" && -n "$lang" ]] || die "usage: get <id> <lang> [text|b64|json]"
  # Call
  local out
  if ! out="$("$GRPCURL_BIN" -plaintext \
      -import-path "$PROTO_DIR" -proto "$PROTO_FILE" \
      -d "{\"textId\":\"$id\",\"lang\":\"$lang\"}" \
      "$TARGET" "$SERVICE/Get")"; then
    echo "$out"
    exit 1
  fi
  case "$mode" in
    json) echo "$out" ;;
    b64)
      # extract base64 "text" field (simple parse; grpcurl JSON is tiny)
      echo "$out" | sed -n 's/.*"text":"\([^"]*\)".*/\1/p'
      ;;
    text)
      local b64; b64="$(echo "$out" | sed -n 's/.*"text":"\([^"]*\)".*/\1/p')"
      if [[ -z "$b64" ]]; then
        echo "(not found or empty)"
      else
        b64dec "$b64"
        echo
      fi
      ;;
    *) die "unknown mode: $mode" ;;
  esac
}

print_usage() {
  cat <<EOF
TextBank CLI (grpcurl wrapper)

USAGE:
  ${0##*/} intern <lang> <text...>
  ${0##*/} get <id> <lang> [text|b64|json]
  ${0##*/} repl
  ${0##*/} help

ENV:
  TEXTBANK_TARGET       ($TARGET)
  TEXTBANK_PROTO_DIR    ($PROTO_DIR)
  TEXTBANK_PROTO_FILE   ($PROTO_FILE)
  GRPCURL_BIN           ($GRPCURL_BIN)

EXAMPLES:
  ${0##*/} intern en "Hello World"
  ${0##*/} get 1 en           # prints decoded text
  ${0##*/} get 1 en b64       # prints base64 payload
  ${0##*/} repl               # interactive mode
EOF
}

repl() {
  echo "Connected to $TARGET  (proto: $PROTO_DIR/$PROTO_FILE)"
  echo "Commands: intern <lang> <text...> | get <id> <lang> [text|b64|json] | quit"
  while true; do
    printf 'textbank> '
    IFS= read -r line || exit 0
    set +e
    # shellcheck disable=SC2086
    set -- $line
    set -e
    case "${1:-}" in
      "" ) continue ;;
      q|quit|exit) exit 0 ;;
      help) print_usage ;;
      intern)
        shift || true
        [[ $# -ge 2 ]] || { echo "usage: intern <lang> <text...>"; continue; }
        lang="$1"; shift
        grpc_intern "$lang" "$*"
        ;;
      get)
        shift || true
        [[ $# -ge 2 ]] || { echo "usage: get <id> <lang> [text|b64|json]"; continue; }
        id="$1"; lang="$2"; mode="${3:-text}"
        grpc_get "$id" "$lang" "$mode"
        ;;
      *)
        echo "unknown command: $1 (type 'help')"
        ;;
    esac
  done
}

main() {
  check_env
  case "${1:-}" in
    intern)
      shift || true
      [[ $# -ge 2 ]] || { print_usage; exit 1; }
      lang="$1"; shift
      grpc_intern "$lang" "$*"
      ;;
    get)
      shift || true
      [[ $# -ge 2 ]] || { print_usage; exit 1; }
      id="$1"; lang="$2"; mode="${3:-text}"
      grpc_get "$id" "$lang" "$mode"
      ;;
    repl)
      repl
      ;;
    help|-h|--help|"")
      print_usage
      ;;
    *)
      print_usage; exit 1;;
  esac
}

main "$@"

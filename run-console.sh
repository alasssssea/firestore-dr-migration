#!/usr/bin/env bash
# Console watchdog: keeps the migration console reachable even if the process dies.
# The root-cause crash (change stream Next racing Close on stop) is fixed and the
# reader goroutine now recovers from panics, but this supervisor is belt-and-braces:
# if the console exits for ANY reason it is relaunched within a couple of seconds so
# the UI never gets stuck at "Failed to fetch".
#
# NOTE: a restart only revives the HTTP console. An in-flight migration job lives in
# the process's memory, so if a crash ever happens mid-migration the job itself is
# gone — you re-launch it from the form. For a planned halt use 暂停 (pause), not a
# process kill.
#
# Usage:  ./run-console.sh [addr]      (default addr :9090)
set -u

BIN="${FSDR_BIN:-./fsdr}"
ADDR="${1:-:9090}"
LOG="${CONSOLE_LOG:-/tmp/console.log}"
# PIDFILE records the *exact* console child PID for each launch, so restart-console.sh
# can target it precisely (kill by PID, not by `pkill -f` pattern that also matches
# unrelated shells — the old source of stray kills / exit 144).
PIDFILE="${CONSOLE_PIDFILE:-/tmp/console.pid}"

cleanup() { rm -f "$PIDFILE"; }
trap cleanup EXIT

echo "[watchdog] $(date -u +%FT%TZ) starting console supervisor on ${ADDR}, binary=${BIN}, log=${LOG}, pidfile=${PIDFILE}" | tee -a "$LOG"

while true; do
  echo "[watchdog] $(date -u +%FT%TZ) launching console..." >> "$LOG"
  # Launch in the background so we can capture the child PID; then wait on it.
  "$BIN" -mode console -metrics-addr "$ADDR" >> "$LOG" 2>&1 &
  child=$!
  echo "$child" > "$PIDFILE"
  wait "$child"
  code=$?
  rm -f "$PIDFILE"
  echo "[watchdog] $(date -u +%FT%TZ) console exited (code=${code}); restarting in 2s" >> "$LOG"
  sleep 2
done

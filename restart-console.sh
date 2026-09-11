#!/usr/bin/env bash
# Safe console restart: pick up a freshly rebuilt ./fsdr WITHOUT the dangers of
# `pkill -f 'fsdr -mode console'` (which also matches unrelated shells — the source
# of stray kills and exit 144).
#
# It does two things the blunt pkill never did:
#   1. Kills the EXACT console child PID recorded by the watchdog in the pidfile,
#      verifying the PID really is a migrate console before signalling it.
#   2. REFUSES to restart while a migration is in flight — an in-flight job lives in
#      the process's memory, so killing the console throws the job away. Pass --force
#      only if you knowingly accept losing the running job.
#
# The watchdog (run-console.sh) relaunches the console within ~2s with the current
# binary, so we only send SIGTERM; we do not start anything ourselves.
#
# Usage:  ./restart-console.sh [--force] [addr]     (default addr :9090)
set -u

FORCE=0
ADDR=":9090"
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    *) ADDR="$arg" ;;
  esac
done

PIDFILE="${CONSOLE_PIDFILE:-/tmp/console.pid}"
BASE="http://localhost${ADDR}"

# --- guard 1: is a migration in flight? ---------------------------------------
status="$(curl -s --max-time 5 "${BASE}/api/status" || true)"
if [[ -n "$status" ]]; then
  active="$(printf '%s' "$status" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
# Non-terminal, non-idle states mean a job is actively using the process memory.
busy = [j for j in d.get("jobs", []) if j.get("state") in
        ("assessing", "initial-load", "live", "verifying", "paused")]
if busy:
    print(", ".join(f'"'"'{j.get("id")}:{j.get("state")}'"'"' for j in busy))
' 2>/dev/null || true)"
  if [[ -n "$active" && "$FORCE" -ne 1 ]]; then
    echo "✋ 拒绝重启：有正在进行的迁移任务 [$active]" >&2
    echo "   重启会杀掉进程、丢失该任务。若确实要丢弃它，请加 --force。" >&2
    echo "   （若只是想暂停，请用界面上的「暂停」，不要重启进程。）" >&2
    exit 2
  fi
  if [[ -n "$active" ]]; then
    echo "⚠ --force：将丢弃正在进行的任务 [$active] 并重启。" >&2
  fi
fi

# --- guard 2: precise PID, verified ------------------------------------------
if [[ ! -f "$PIDFILE" ]]; then
  echo "找不到 pidfile ($PIDFILE)。watchdog(run-console.sh)是否在跑、是否已用新版脚本启动？" >&2
  exit 1
fi
pid="$(cat "$PIDFILE" 2>/dev/null)"
if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ ]]; then
  echo "pidfile 内容异常：'$pid'" >&2
  exit 1
fi
# Only kill it if the PID really is our migrate console (guards against a stale
# pidfile whose PID got recycled by an unrelated process).
args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
if [[ "$args" != *"fsdr"* || "$args" != *"-mode console"* ]]; then
  echo "PID $pid 不是 fsdr console（pidfile 可能已过期）：'$args'。拒绝 kill。" >&2
  exit 1
fi

kill -TERM "$pid"
echo "已向 console 子进程 (pid $pid) 发送 SIGTERM；watchdog 将在约 2s 内用当前 fsdr 重启。"

# Bounded escalation: if graceful shutdown hangs, the watchdog's `wait` blocks and the
# console never relaunches. Give it up to 8s to exit, then SIGKILL that exact PID.
for _ in $(seq 1 8); do
  kill -0 "$pid" 2>/dev/null || break
  sleep 1
done
if kill -0 "$pid" 2>/dev/null; then
  echo "pid $pid 未在 8s 内退出，发送 SIGKILL。" >&2
  kill -KILL "$pid" 2>/dev/null
fi

# --- wait for the relaunched console to answer -------------------------------
for _ in $(seq 1 15); do
  sleep 1
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "${BASE}/" 2>/dev/null || true)"
  if [[ "$code" == "200" ]]; then
    echo "✅ console 已重启并就绪 (HTTP 200) @ ${BASE}/"
    exit 0
  fi
done
echo "⚠ 等待超时：console 尚未在 ${BASE}/ 返回 200，请查看 ${CONSOLE_LOG:-/tmp/console.log}" >&2
exit 1

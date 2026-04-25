#!/usr/bin/env bash
# 在 tmux 里启动 happy，SSH 断开也不会挂。
# 用法：
#   ./start-happy.sh            # 新建或附着到名为 "happy" 的 tmux 会话
#   tmux a -t happy             # 之后重新附着
#   Ctrl+b 然后 d               # 在 tmux 里按这个断开但不杀会话
#
# 注：RSS 突发警报监控器 alert_monitor.py 由 systemd 托管
# (news-alert-monitor.service)，不再在 tmux 里启动。
set -euo pipefail

SESSION="${HAPPY_TMUX_SESSION:-happy}"
DIR="$(cd "$(dirname "$0")" && pwd)"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "附着到已有会话: $SESSION"
    exec tmux attach -t "$SESSION"
fi

echo "新建 tmux 会话: $SESSION"

# 窗口0：happy 主进程
tmux new-session -d -s "$SESSION" -n "happy" -x 220 -y 50 "cd \"$DIR\" && happy"

echo "tmux 会话已启动："
echo "  窗口0 'happy'  — Happy AI 主进程"
echo ""
echo "附着：tmux attach -t $SESSION"
echo "(alert_monitor 由 systemd 管理: systemctl status news-alert-monitor)"

exec tmux attach -t "$SESSION"

#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "请用 root 执行：sudo bash scripts/bootstrap_debian.sh"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${1:-/opt/debian-ops-agent}"
RUN_USER="${2:-opsbot}"

echo "[1/6] 安装 Python 运行环境..."
apt-get update
apt-get install -y python3 python3-venv python3-pip

echo "[2/6] 创建运行用户（如不存在）..."
if ! id -u "${RUN_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --shell /usr/sbin/nologin "${RUN_USER}"
fi

echo "[3/6] 同步项目到 ${TARGET_DIR} ..."
mkdir -p "${TARGET_DIR}"
cp -a "${PROJECT_DIR}/." "${TARGET_DIR}/"

echo "[4/6] 创建虚拟环境并安装依赖..."
python3 -m venv "${TARGET_DIR}/.venv"
"${TARGET_DIR}/.venv/bin/pip" install -U pip
"${TARGET_DIR}/.venv/bin/pip" install -r "${TARGET_DIR}/requirements.txt"

echo "[5/6] 初始化环境变量文件..."
if [[ ! -f "${TARGET_DIR}/.env" ]]; then
  cp "${TARGET_DIR}/.env.example" "${TARGET_DIR}/.env"
fi

echo "[6/6] 设置目录权限..."
chown -R "${RUN_USER}:${RUN_USER}" "${TARGET_DIR}"

cat <<EOF

初始化完成。请继续执行：

1) 编辑环境变量：
   sudo -u ${RUN_USER} nano ${TARGET_DIR}/.env
   - 至少设置 OPS_AGENT_APPROVAL_TOKEN

2) 安装 systemd 服务：
   sudo cp ${TARGET_DIR}/deploy/ops-agent.service /etc/systemd/system/ops-agent.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now ops-agent

3) 查看状态：
   sudo systemctl status ops-agent --no-pager
   curl -s http://127.0.0.1:8088/health

EOF

#!/bin/bash
# ============================================================================
# 港股強勢股篩選器 V5.6.10 Milon - LXC 自動安裝腳本
# ============================================================================
# 
# 用法:
#   1. 在 PVE 上創建 LXC 容器
#   2. 進入容器: pct enter <CT_ID>
#   3. 下載此腳本: curl -O https://raw.githubusercontent.com/your-repo/install.sh
#   4. 執行安裝: chmod +x install.sh && ./install.sh
#
# ============================================================================

set -e

# ============================================================================
# 顏色定義
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ============================================================================
# 變量定義
# ============================================================================
APP_DIR="/app"
DATA_DIR="/app/data"
LOG_DIR="/app/logs"
CONFIG_FILE="/app/config.json"
SERVICE_FILE="/etc/systemd/system/hk-stock.service"
APP_VERSION="5.6.10"

# ============================================================================
# 日誌函數
# ============================================================================
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${CYAN}[✓]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# ============================================================================
# 檢查 Root 權限
# ============================================================================
check_root() {
    log_step "檢查用戶權限..."
    
    if [ "$EUID" -ne 0 ]; then
        log_error "請使用 root 權限運行此腳本！"
        echo ""
        echo "使用方法: sudo $0"
        echo ""
        exit 1
    fi
    
    log_success "Root 權限確認"
}

# ============================================================================
# 檢查系統環境
# ============================================================================
check_system() {
    log_step "檢查系統環境..."
    
    # 檢查操作系統
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS_NAME="$NAME"
        OS_VERSION="$VERSION"
        log_info "操作系統: $OS_NAME $OS_VERSION"
    else
        log_warn "無法檢測操作系統，繼續安裝..."
        OS_NAME="Unknown"
    fi
    
    # 檢查 Python
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        PYTHON_FULL=$(python3 --version 2>&1)
        log_info "Python 版本: $PYTHON_FULL"
        PYTHON_INSTALLED=1
    else
        log_warn "未檢測到 Python3，將進行安裝"
        PYTHON_INSTALLED=0
    fi
    
    # 檢查 Git
    if command -v git &> /dev/null; then
        GIT_VERSION=$(git --version | cut -d' ' -f3)
        log_info "Git 版本: $GIT_VERSION"
        GIT_INSTALLED=1
    else
        log_warn "未檢測到 Git，將進行安裝"
        GIT_INSTALLED=0
    fi
    
    # 檢查 curl
    if command -v curl &> /dev/null; then
        CURL_INSTALLED=1
    else
        log_warn "未檢測到 curl，將進行安裝"
        CURL_INSTALLED=0
    fi
    
    # 檢查 wget
    if command -v wget &> /dev/null; then
        WGET_INSTALLED=1
    else
        log_warn "未檢測到 wget，將進行安裝"
        WGET_INSTALLED=0
    fi
    
    log_success "系統環境檢查完成"
}

# ============================================================================
# 配置 apt 源
# ============================================================================
configure_apt_source() {
    log_step "配置軟件源..."
    
    # 檢測是否為 Debian/Ubuntu
    if command -v apt-get &> /dev/null; then
        if [ -f /etc/apt/sources.list ]; then
            # 確保 sources.list 存在
            if ! grep -q "deb.debian.org" /etc/apt/sources.list 2>/dev/null; then
                log_info "更新為官方 Debian 源..."
                cat > /etc/apt/sources.list << 'APTEOF'
deb http://deb.debian.org/debian bookworm main contrib non-free
deb http://deb.debian.org/debian-security bookworm-security main contrib non-free
deb http://deb.debian.org/debian bookworm-updates main contrib non-free
APTEOF
            fi
        fi
        log_success "軟件源配置完成"
    fi
}

# ============================================================================
# 更新系統
# ============================================================================
update_system() {
    log_step "更新系統軟件包..."
    
    export DEBIAN_FRONTEND=noninteractive
    
    if command -v apt-get &> /dev/null; then
        apt-get update -qq
        apt-get upgrade -y -qq
        log_info "系統更新完成"
    elif command -v dnf &> /dev/null; then
        dnf check-update -qq || true
        dnf upgrade -y -q
        log_info "系統更新完成"
    elif command -v yum &> /dev/null; then
        yum check-update || true
        yum upgrade -y -q
        log_info "系統更新完成"
    fi
    
    log_success "系統更新完成"
}

# ============================================================================
# 安裝系統依賴
# ============================================================================
install_system_dependencies() {
    log_step "安裝系統依賴..."
    
    export DEBIAN_FRONTEND=noninteractive
    
    if command -v apt-get &> /dev/null; then
        apt-get install -y -qq \
            python3 \
            python3-pip \
            python3-venv \
            python3-dev \
            python3-setuptools \
            git \
            curl \
            wget \
            vim \
            htop \
            iftop \
            iotop \
            net-tools \
            ca-certificates \
            tzdata \
            libc6-dev \
            libgomp1 \
            liblz4-1 \
            libstdc++6 \
            zlib1g \
            libncurses5 \
            libbz2-1.0 \
            libsqlite3-0 \
            libssl3 \
            fonts-dejavu-core \
            locales \
            && apt-get clean \
            && rm -rf /var/lib/apt/lists/*
    elif command -v dnf &> /dev/null; then
        dnf install -y -q \
            python3 \
            python3-pip \
            git \
            curl \
            wget \
            vim \
            htop \
            net-tools \
            ca-certificates
    elif command -v yum &> /dev/null; then
        yum install -y -q \
            python3 \
            python3-pip \
            git \
            curl \
            wget \
            vim \
            htop \
            net-tools
    fi
    
    log_success "系統依賴安裝完成"
}

# ============================================================================
# 配置時區
# ============================================================================
configure_timezone() {
    log_step "配置時區為 Asia/Hong_Kong..."
    
    # 安裝 tzdata (如果不存在)
    if [ ! -f /usr/share/zoneinfo/Asia/Hong_Kong ]; then
        apt-get install -y -qq tzdata > /dev/null 2>&1
    fi
    
    # 設置時區
    if [ -f /usr/share/zoneinfo/Asia/Hong_Kong ]; then
        echo "Asia/Hong_Kong" > /etc/timezone
        ln -sf /usr/share/zoneinfo/Asia/Hong_Kong /etc/localtime
        
        # 配置 timedatectl
        if command -v timedatectl &> /dev/null; then
            timedatectl set-timezone Asia/Hong_Kong
        fi
        
        # 配置 NTP
        if command -v timedatectl &> /dev/null; then
            timedatectl set-ntp true
        fi
        
        CURRENT_TZ=$(cat /etc/timezone)
        log_success "時區已設置為: $CURRENT_TZ"
    else
        log_warn "香港時區文件不存在，使用 UTC"
    fi
}

# ============================================================================
# 創建目錄結構
# ============================================================================
create_directories() {
    log_step "創建應用目錄結構..."
    
    # 創建目錄
    mkdir -p "$APP_DIR"
    mkdir -p "$DATA_DIR"
    mkdir -p "$LOG_DIR"
    mkdir -p "$APP_DIR/venv"
    
    # 設置權限
    chmod -R 755 "$APP_DIR"
    chmod -R 755 "$DATA_DIR"
    chmod -R 755 "$LOG_DIR"
    
    log_success "目錄結構創建完成"
    log_info "  - 應用目錄: $APP_DIR"
    log_info "  - 數據目錄: $DATA_DIR"
    log_info "  - 日誌目錄: $LOG_DIR"
}

# ============================================================================
# 配置 Python 環境
# ============================================================================
configure_python() {
    log_step "配置 Python 環境..."
    
    # 確保 pip 可用
    if ! command -v pip3 &> /dev/null; then
        log_info "安裝 pip3..."
        curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
        python3 /tmp/get-pip.py
        rm -f /tmp/get-pip.py
    fi
    
    # 升級 pip
    log_info "升級 pip..."
    python3 -m pip install --upgrade pip -q
    
    # 配置 pip 緩存
    mkdir -p /root/.cache/pip
    
    log_success "Python 環境配置完成"
}

# ============================================================================
# 創建虛擬環境
# ============================================================================
create_venv() {
    log_step "創建 Python 虛擬環境..."
    
    if [ -d "$APP_DIR/venv" ]; then
        log_info "虛擬環境已存在，移除舊版本..."
        rm -rf "$APP_DIR/venv"
    fi
    
    python3 -m venv "$APP_DIR/venv"
    
    # 驗證虛擬環境
    if [ -f "$APP_DIR/venv/bin/activate" ]; then
        log_success "虛擬環境創建成功"
        
        # 獲取 Python 路徑
        PYTHON_PATH="$APP_DIR/venv/bin/python"
        PIP_PATH="$APP_DIR/venv/bin/pip"
        
        log_info "Python 路徑: $PYTHON_PATH"
        log_info "Python 版本: $($PYTHON_PATH --version 2>&1)"
    else
        log_error "虛擬環境創建失敗！"
        exit 1
    fi
}

# ============================================================================
# 安裝 Python 依賴
# ============================================================================
install_python_dependencies() {
    log_step "安裝 Python 依賴..."
    
    # 激活虛擬環境
    source "$APP_DIR/venv/bin/activate"
    
    # 安裝依賴
    log_info "正在安裝 streamlit, yfinance, pandas, numpy, requests..."
    
    pip install --no-cache-dir -q \
        streamlit>=1.28.0 \
        yfinance>=0.2.36 \
        pandas>=2.0.0 \
        numpy>=1.24.0 \
        requests>=2.31.0
    
    # 驗證安裝
    python -c "import streamlit; import yfinance; import pandas; import numpy; import requests; print('✓ 所有依賴安裝成功')" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        log_success "Python 依賴安裝完成"
    else
        log_error "Python 依賴安裝驗證失敗！"
        exit 1
    fi
    
    # 退出虛擬環境
    deactivate
}

# ============================================================================
# 創建配置文件
# ============================================================================
create_config() {
    log_step "創建配置文件..."
    
    cat > "$CONFIG_FILE" << 'CONFIGEOF'
{
    "rate_limit_per_min": 120,
    "max_retries": 3,
    "data_retention_days": 365,
    "min_data_points": 30,
    "clean_data": true,
    "debug_mode": false,
    "log_level": "INFO",
    "cache_enabled": true,
    "cache_ttl_hours": 24,
    "workers": 4,
    "timeout_seconds": 30,
    "batch_size": 1000,
    "async_concurrent": 10,
    "max_cache_size": 2000,
    "max_cache_memory_mb": 200.0,
    "default_initial_capital": 100000.0,
    "default_position_size": 0.1,
    "default_stop_loss": 0.05,
    "default_take_profit": 0.15,
    "auto_update_enabled": false,
    "auto_update_mode": "scheduled",
    "auto_update_time": "12:00",
    "auto_update_interval_hours": 6,
    "auto_update_max_stocks": 676,
    "auto_update_outdated_days": 1,
    "auto_update_notify": false,
    "notification_enabled": true,
    "telegram_enabled": false,
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "telegram_parse_mode": "HTML",
    "line_enabled": false,
    "line_access_token": "",
    "line_notify_token": "",
    "whatsapp_enabled": false,
    "whatsapp_account_sid": "",
    "whatsapp_auth_token": "",
    "whatsapp_from_number": "",
    "whatsapp_to_number": "",
    "email_enabled": false,
    "email_smtp_server": "smtp.gmail.com",
    "email_smtp_port": 587,
    "email_sender": "",
    "email_password": "",
    "email_recipients": "",
    "email_use_tls": true,
    "webhook_enabled": false,
    "webhook_url": "",
    "webhook_method": "POST",
    "webhook_headers": "",
    "default_min_5d_return": 5.0,
    "default_min_3d_return": 8.0,
    "default_volume_ratio": 1.5,
    "default_price_strength": 80.0,
    "default_max_rsi": 80,
    "db_busy_timeout": 60000,
    "db_pool_size": 10,
    "db_max_retries": 5,
    "db_retry_base_delay": 0.1,
    "db_retry_max_delay": 10.0
}
CONFIGEOF
    
    chmod 644 "$CONFIG_FILE"
    
    log_success "配置文件創建完成: $CONFIG_FILE"
}

# ============================================================================
# 創建 systemd 服務
# ============================================================================
create_systemd_service() {
    log_step "創建 systemd 服務..."
    
    cat > "$SERVICE_FILE" << 'SERVICEEOF'
[Unit]
Description=HK Stock Screener V5.6.10 Milon
Documentation=https://github.com/your-repo/hk-stock-screener
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/app
Environment="PATH=/app/venv/bin"
Environment="PYTHONUNBUFFERED=1"
Environment="TZ=Asia/Hong_Kong"
ExecStart=/app/venv/bin/streamlit run /app/hk_stocks_v5.6.10.py \
    --server.port=8501 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --logger.level=INFO
Restart=always
RestartSec=10
StartLimitBurst=5
StartLimitInterval=60

# 日誌配置
StandardOutput=append:/app/logs/stdout.log
StandardError=append:/app/logs/stderr.log

# 安全加固
ProtectSystem=strict
ReadWritePaths=/app/data /app/logs
NoNewPrivileges=true
PrivateTmp=true
DevicePolicy=closed
ProtectHostname=true
ProtectClock=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true

[Install]
WantedBy=multi-user.target
SERVICEEOF
    
    # 重新加載 systemd
    systemctl daemon-reload
    
    log_success "systemd 服務創建完成: $SERVICE_FILE"
}

# ============================================================================
# 創建啟動腳本
# ============================================================================
create_startup_script() {
    log_step "創建啟動腳本..."
    
    cat > "$APP_DIR/start.sh" << 'STARTEOF'
#!/bin/bash
# ============================================================================
# 港股強勢股篩選器 - 手動啟動腳本
# ============================================================================

APP_DIR="/app"
LOG_DIR="/app/logs"

# 創建日誌目錄
mkdir -p "$LOG_DIR"

# 配置日誌文件
LOG_FILE="$LOG_DIR/hk_stock_$(date +%Y%m%d).log"

# 顯示啟動信息
echo "========================================"
echo "  港股強勢股篩選器 V5.6.10 Milon"
echo "========================================"
echo ""
echo "📁 數據目錄: $APP_DIR"
echo "📁 日誌目錄: $LOG_DIR"
echo "📁 日誌文件: $LOG_FILE"
echo ""

# 激活虛擬環境
source "$APP_DIR/venv/bin/activate"

# 啟動 Streamlit
echo "🚀 啟動服務..."
exec streamlit run "$APP_DIR/hk_stocks_v5.6.10.py" \
    --server.port 8501 \
    --server.headless true \
    --browser.gatherUsageStats false \
    --logger.level INFO \
    2>&1 | tee -a "$LOG_FILE"
STARTEOF
    
    chmod +x "$APP_DIR/start.sh"
    
    log_success "啟動腳本創建完成: $APP_DIR/start.sh"
}

# ============================================================================
# 創建健康檢查腳本
# ============================================================================
create_healthcheck() {
    log_step "創建健康檢查腳本..."
    
    cat > "$APP_DIR/healthcheck.sh" << 'HEALTHEOF'
#!/bin/bash
# ============================================================================
# 健康檢查腳本
# ============================================================================

# 檢查 Streamlit 進程
if pgrep -f "streamlit run" > /dev/null; then
    # 檢查端口監聽
    if ss -tuln | grep -q ":8501 "; then
        echo "OK: Service is running and listening on port 8501"
        exit 0
    else
        echo "WARNING: Process running but port 8501 not listening"
        exit 1
    fi
else
    echo "CRITICAL: Streamlit process not found"
    exit 2
fi
HEALTHEOF
    
    chmod +x "$APP_DIR/healthcheck.sh"
    
    log_success "健康檢查腳本創建完成: $APP_DIR/healthcheck.sh"
}

# ============================================================================
# 創建監控腳本
# ============================================================================
create_monitor_script() {
    log_step "創建監控腳本..."
    
    cat > "$APP_DIR/monitor.sh" << 'MONITOREOF'
#!/bin/bash
# ============================================================================
# 監控腳本 - 監測服務狀態
# ============================================================================

LOG_FILE="/app/logs/monitor.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# 監控循環
while true; do
    # 檢查進程
    if ! pgrep -f "streamlit run" > /dev/null; then
        log "WARN: Streamlit process not found, restarting..."
        systemctl restart hk-stock
        sleep 10
        continue
    fi
    
    # 檢查內存使用
    MEM_USAGE=$(free | grep Mem | awk '{printf "%.0f", $3/$2 * 100}')
    if [ "$MEM_USAGE" -gt 90 ]; then
        log "WARN: High memory usage: ${MEM_USAGE}%"
    fi
    
    # 每 60 秒檢查一次
    sleep 60
done
MONITOREOF
    
    chmod +x "$APP_DIR/monitor.sh"
    
    log_success "監控腳本創建完成: $APP_DIR/monitor.sh"
}

# ============================================================================
# 創建備份腳本
# ============================================================================
create_backup_script() {
    log_step "創建備份腳本..."
    
    cat > "$APP_DIR/backup.sh" << 'BACKUPEOF'
#!/bin/bash
# ============================================================================
# 備份腳本
# ============================================================================

BACKUP_DIR="/app/backup"
DATA_DIR="/app/data"
DATE=$(date +%Y%m%d_%H%M%S)

# 創建備份目錄
mkdir -p "$BACKUP_DIR"

# 備份數據庫
if [ -f "$DATA_DIR/hk_stocks.db" ]; then
    cp "$DATA_DIR/hk_stocks.db" "$BACKUP_DIR/hk_stocks_$DATE.db"
    gzip "$BACKUP_DIR/hk_stocks_$DATE.db"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 數據庫備份完成: hk_stocks_$DATE.db.gz" >> /app/logs/backup.log
fi

# 清理舊備份 (保留最近 7 天)
find "$BACKUP_DIR" -name "*.gz" -mtime +7 -delete

echo "備份完成"
BACKUPEOF
    
    chmod +x "$APP_DIR/backup.sh"
    
    # 添加 cron 任務 (每天凌晨 3 點執行)
    if command -v crontab &> /dev/null; then
        echo "0 3 * * * /app/backup.sh >> /app/logs/backup.log 2>&1" | crontab -
        log_info "備份定時任務已設置 (每天凌晨 3:00)"
    fi
    
    log_success "備份腳本創建完成: $APP_DIR/backup.sh"
}

# ============================================================================
# 配置防火牆
# ============================================================================
configure_firewall() {
    log_step "配置防火牆..."
    
    if command -v ufw &> /dev/null; then
        ufw allow 8501/tcp comment 'Streamlit'
        ufw --force enable
        log_info "UFW 防火牆允許端口 8501"
        log_success "UFW 防火牆配置完成"
    elif command -v firewall-cmd &> /dev/null; then
        firewall-cmd --permanent --add-port=8501/tcp
        firewall-cmd --reload
        log_success "Firewalld 防火牆配置完成"
    else
        log_warn "未檢測到防火牆軟件，請手動配置網絡安全"
    fi
}

# ============================================================================
# 優化系統參數
# ============================================================================
optimize_system() {
    log_step "優化系統參數..."
    
    # 增加文件描述符限制
    if ! grep -q "65535" /etc/security/limits.conf 2>/dev/null; then
        echo "* soft nofile 65535" >> /etc/security/limits.conf
        echo "* hard nofile 65535" >> /etc/security/limits.conf
        log_info "文件描述符限制已設置"
    fi
    
    # 優化網絡參數
    if ! grep -q "net.core.somaxconn" /etc/sysctl.conf 2>/dev/null; then
        echo "net.core.somaxconn = 1024" >> /etc/sysctl.conf
        echo "net.ipv4.tcp_max_syn_backlog = 1024" >> /etc/sysctl.conf
        log_info "網絡參數已優化"
    fi
    
    # 應用配置
    sysctl -p 2>/dev/null || true
    
    log_success "系統參數優化完成"
}

# ============================================================================
# 啟動服務
# ============================================================================
start_service() {
    log_step "啟動服務..."
    
    # 重新加載 systemd
    systemctl daemon-reload
    
    # 啟用服務 (開機自啟)
    systemctl enable hk-stock.service
    
    # 啟動服務
    systemctl start hk-stock.service
    
    # 等待服務啟動
    sleep 3
    
    # 檢查服務狀態
    if systemctl is-active --quiet hk-stock.service; then
        log_success "服務啟動成功！"
        echo ""
        echo "========================================"
        systemctl status hk-stock.service --no-pager --no-legend
        echo "========================================"
    else
        log_error "服務啟動失敗！"
        echo ""
        echo "========================================"
        echo "日誌輸出:"
        journalctl -u hk-stock.service --no-pager -n 50
        echo "========================================"
        exit 1
    fi
}

# ============================================================================
# 驗證部署
# ============================================================================
verify_deployment() {
    log_step "驗證部署..."
    
    VERIFY_PASS=1
    
    # 檢查 1: systemd 服務
    if systemctl is-active --quiet hk-stock.service; then
        log_success "[1/4] systemd 服務運行正常"
    else
        log_error "[1/4] systemd 服務未運行"
        VERIFY_PASS=0
    fi
    
    # 檢查 2: 端口監聽
    sleep 2
    if ss -tuln | grep -q ":8501 "; then
        log_success "[2/4] 端口 8501 監聽正常"
    else
        log_error "[2/4] 端口 8501 未監聽"
        VERIFY_PASS=0
    fi
    
    # 檢查 3: 健康檢查端點
    if command -v curl &> /dev/null; then
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8501/_stcore/health 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "200" ]; then
            log_success "[3/4] 健康檢查通過 (HTTP 200)"
        else
            log_warn "[3/4] 健康檢查返回 HTTP $HTTP_CODE (可能還在啟動中)"
        fi
    else
        log_info "[3/4] 跳過 HTTP 檢查 (curl 未安裝)"
    fi
    
    # 檢查 4: 進程運行
    if pgrep -f "streamlit run" > /dev/null; then
        log_success "[4/4] Streamlit 進程運行正常"
    else
        log_error "[4/4] Streamlit 進程未運行"
        VERIFY_PASS=0
    fi
    
    if [ $VERIFY_PASS -eq 1 ]; then
        log_success "所有驗證通過！"
        return 0
    else
        log_warn "部分驗證未通過，請檢查日誌"
        return 1
    fi
}

# ============================================================================
# 顯示完成信息
# ============================================================================
show_completion() {
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                                                              ║${NC}"
    echo -e "${GREEN}║            🎉 安裝完成！港股強勢股篩選器 🎉                ║${NC}"
    echo -e "${GREEN}║                                                              ║${NC}"
    echo -e "${GREEN}║                    版本: V${APP_VERSION} Milon                       ║${NC}"
    echo -e "${GREEN}║                                                              ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    # 獲取 IP 地址
    HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "<IP>")
    
    echo -e "${CYAN}📱 訪問地址:${NC}"
    echo -e "   🌐 Web UI: http://${HOST_IP}:8501"
    echo ""
    
    echo -e "${CYAN}📋 管理命令:${NC}"
    echo "   查看狀態: systemctl status hk-stock"
    echo "   查看日誌: journalctl -u hk-stock -f"
    echo "   重啟服務: systemctl restart hk-stock"
    echo "   停止服務: systemctl stop hk-stock"
    echo ""
    
    echo -e "${CYAN}📁 文件位置:${NC}"
    echo "   程序目錄: $APP_DIR"
    echo "   數據目錄: $DATA_DIR"
    echo "   日誌目錄: $LOG_DIR"
    echo "   配置目錄: $CONFIG_FILE"
    echo ""
    
    echo -e "${CYAN}🔧 實用工具:${NC}"
    echo "   啟動腳本: $APP_DIR/start.sh"
    echo "   健康檢查: $APP_DIR/healthcheck.sh"
    echo "   監控腳本: $APP_DIR/monitor.sh"
    echo "   備份腳本: $APP_DIR/backup.sh"
    echo ""
    
    echo -e "${YELLOW}💡 提示:${NC}"
    echo "   - 首次運行可能需要下載股票數據，請耐心等待"
    echo "   - 可在 Web UI 中配置 Telegram/LINE 等通知"
    echo "   - 建議定期查看日誌監控系統狀態"
    echo ""
    
    echo -e "${GREEN}✅ Happy Trading! 🚀${NC}"
    echo ""
}

# ============================================================================
# 主函數
# ============================================================================
main() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║     港股強勢股篩選器 V${APP_VERSION} Milon - LXC 安裝腳本        ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    # 執行安裝步驟
    check_root
    check_system
    configure_apt_source
    update_system
    install_system_dependencies
    configure_timezone
    create_directories
    configure_python
    create_venv
    install_python_dependencies
    create_config
    create_systemd_service
    create_startup_script
    create_healthcheck
    create_monitor_script
    create_backup_script
    configure_firewall
    optimize_system
    start_service
    
    # 驗證部署
    if verify_deployment; then
        show_completion
    else
        log_warn "部署驗證未完全通過，但服務已啟動"
        log_info "請訪問 http://<容器IP>:8501 確認 Web UI 是否正常"
        echo ""
    fi
}

# ============================================================================
# 執行主函數
# ============================================================================
main "$@"

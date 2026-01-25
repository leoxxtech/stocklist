"""
港股強勢股篩選器 - V5.6.10 (完整修復版)
=====================================
Version: 5.6.10
LastUpdate: 2026-01-25
Author: AI Assistant

【修復內容】
1. SQLite 並發連接支持
   - WAL 模式 (Write-Ahead Logging)
   - busy_timeout 設置為 60000ms
   - 連接池管理 (10個連接)
   - 線程安全的連接獲取
   - 自動重試機制 (最多5次，指數退避)

2. 修復欄位數量不匹配
   - stock_indicators 表移除了不存在的 batch_id 欄位
   - 確保 INSERT 語句的欄位數和值數量一致

3. 背景自動更新指定時間功能
   - 改為每天固定時間更新 (預設 12:00)
   - 預設不啟用自動更新
   - 保留手動執行功能

4. 修復 WhatsAppProvider.test_connection 返回值語法錯誤

5. 修復 KeyError: 'total_updates'
   - 使用 .get() 配合預設值存取嵌套字典
"""

# ============================================================================
# 0. 導入所有必要模組
# ============================================================================
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import time
import os
import sqlite3
import json
import logging
import threading
import hashlib
import re
import unittest
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple, Any, Union, Callable
from pathlib import Path
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from contextlib import contextmanager

# ============================================================================
# 1. 版本控制標記
# ============================================================================
APP_VERSION = "5.6.10"
APP_VERSION_DATE = "2026-01-25"
APP_AUTHOR = "AI Assistant"
SCHEMA_VERSION = 9

# ============================================================================
# 2. 錯誤碼定義
# ============================================================================
class ErrorCode:
    SUCCESS = 0
    UNKNOWN_ERROR = 1
    INVALID_INPUT = 2
    FILE_NOT_FOUND = 3
    PERMISSION_DENIED = 4
    TIMEOUT = 5
    
    INVALID_STOCK_CODE = 100
    INVALID_TICKER = 101
    STOCK_NOT_FOUND = 102
    DUPLICATE_STOCK = 103
    
    INVALID_DATA_FORMAT = 200
    DATA_NOT_FOUND = 201
    DATA_CORRUPTED = 202
    DATA_OUT_OF_RANGE = 203
    
    DB_CONNECTION_FAILED = 300
    DB_QUERY_FAILED = 301
    DB_INSERT_FAILED = 302
    DB_UPDATE_FAILED = 303
    DB_MIGRATION_FAILED = 304
    DB_LOCKED = 305
    DB_CONSTRAINT_VIOLATION = 306
    
    NETWORK_ERROR = 400
    API_RATE_LIMIT = 401
    API_TIMEOUT = 402
    ASYNC_ERROR = 403
    
    INVALID_PARAMETER = 500
    PARAMETER_OUT_OF_RANGE = 501
    MISSING_REQUIRED_PARAM = 502
    
    CACHE_ERROR = 600
    CACHE_FULL = 601
    CACHE_EXPIRED = 602
    
    BACKTEST_ERROR = 700
    BACKTEST_INVALID_PARAMS = 701
    BACKTEST_NO_DATA = 702
    
    ALERT_ERROR = 800
    ALERT_INVALID_CONDITION = 801
    ALERT_DELETED = 802
    
    NOTIFICATION_ERROR = 900
    NOTIFICATION_CONFIG_ERROR = 901
    NOTIFICATION_SEND_FAILED = 902
    TELEGRAM_ERROR = 910
    LINE_ERROR = 911
    WHATSAPP_ERROR = 912
    EMAIL_ERROR = 913
    WEBHOOK_ERROR = 914
    
    BACKGROUND_TASK_ERROR = 1000
    BACKGROUND_TASK_RUNNING = 1001
    BACKGROUND_TASK_STOPPED = 1002
    BACKGROUND_TASK_CANCELLED = 1003
    
    @classmethod
    def get_message(cls, code: int) -> str:
        messages = {
            0: "成功", 1: "未知錯誤", 2: "無效輸入", 3: "檔案不存在",
            4: "權限不足", 5: "超時",
            100: "無效的股票代碼", 101: "無效的股票報價", 102: "股票不存在", 103: "重複的股票",
            200: "無效的數據格式", 201: "數據不存在", 202: "數據已損壞", 203: "數據超出範圍",
            300: "數據庫連接失敗", 301: "數據庫查詢失敗", 302: "數據庫插入失敗", 303: "數據庫更新失敗",
            304: "資料庫遷移失敗", 305: "資料庫被鎖定", 306: "資料庫約束衝突",
            400: "網路錯誤", 401: "API 速率限制", 402: "API 超時", 403: "異步操作錯誤",
            500: "無效參數", 501: "參數超出範圍", 502: "缺少必要參數",
            600: "緩存錯誤", 601: "緩存已滿", 602: "緩存已過期",
            700: "回測錯誤", 701: "回測參數無效", 702: "回測無數據",
            800: "警報錯誤", 801: "警報條件無效", 802: "警報已刪除",
            900: "通知錯誤", 901: "通知配置錯誤", 902: "通知發送失敗",
            910: "Telegram 錯誤", 911: "LINE 錯誤", 912: "WhatsApp 錯誤", 913: "Email 錯誤", 914: "Webhook 錯誤",
            1000: "背景任務錯誤", 1001: "背景任務已在運行", 1002: "背景任務已停止", 1003: "背景任務已取消"
        }
        return messages.get(code, f"未知錯誤碼: {code}")


class AppError(Exception):
    def __init__(self, code: int, message: str = None, details: str = None):
        self.code = code
        self.message = message or ErrorCode.get_message(code)
        self.details = details
        super().__init__(self.message)
    
    def __str__(self):
        if self.details:
            return f"[錯誤碼 {self.code}] {self.message} - {self.details}"
        return f"[錯誤碼 {self.code}] {self.message}"
    
    def to_dict(self) -> dict:
        return {
            'error_code': self.code,
            'error_message': self.message,
            'error_details': self.details
        }


# ============================================================================
# 3. 全局控制變數
# ============================================================================
_PAUSE_EVENT = None
_STOP_EVENT = None
_UPDATE_STATE = {
    'is_paused': False,
    'is_stopped': False,
    'paused_at': None,
    'stopped_at': None
}

config = None
cache_manager = None
yf_downloader = None
notification_manager = None
background_task_manager = None
stocks_df_global = None

BACKGROUND_TASK_PROGRESS = {
    'is_running': False,
    'current_stock': None,
    'current_stock_name': None,
    'processed': 0,
    'total': 0,
    'updated': 0,
    'failed': 0,
    'qualified': 0,
    'start_time': None,
    'logs': [],
    'last_update': None
}

_bg_progress_lock = threading.Lock()


def init_control_events():
    global _PAUSE_EVENT, _STOP_EVENT
    if _PAUSE_EVENT is None:
        _PAUSE_EVENT = threading.Event()
        _PAUSE_EVENT.set()
    if _STOP_EVENT is None:
        _STOP_EVENT = threading.Event()
        _STOP_EVENT.clear()


def pause_update():
    global _UPDATE_STATE
    init_control_events()
    if not _UPDATE_STATE['is_paused']:
        _PAUSE_EVENT.clear()
        _UPDATE_STATE['is_paused'] = True
        _UPDATE_STATE['paused_at'] = datetime.now()


def resume_update():
    global _UPDATE_STATE
    init_control_events()
    if _UPDATE_STATE['is_paused']:
        _PAUSE_EVENT.set()
        _UPDATE_STATE['is_paused'] = False
        _UPDATE_STATE['paused_at'] = None


def stop_update():
    global _UPDATE_STATE
    init_control_events()
    if not _UPDATE_STATE['is_stopped']:
        _STOP_EVENT.set()
        _UPDATE_STATE['is_stopped'] = True
        _UPDATE_STATE['stopped_at'] = datetime.now()


def reset_control_state():
    global _UPDATE_STATE
    init_control_events()
    _PAUSE_EVENT.set()
    _STOP_EVENT.clear()
    _UPDATE_STATE = {
        'is_paused': False,
        'is_stopped': False,
        'paused_at': None,
        'stopped_at': None
    }


def check_pause_or_stop() -> Tuple[bool, str]:
    init_control_events()
    
    if _STOP_EVENT.is_set():
        return False, "已停止"
    
    if not _PAUSE_EVENT.is_set():
        return False, "已暫停"
    
    return True, ""


# ============================================================================
# 4. 日誌系統
# ============================================================================
class LoggerManager:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if LoggerManager._initialized:
            return
        
        self.log_dir = Path("logs")
        self.log_dir.mkdir(exist_ok=True)
        
        self.log_file = self.log_dir / f"hk_stock_screener_{datetime.now().strftime('%Y%m%d')}.log"
        
        self.logger = logging.getLogger("HKStockScreener")
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers = []
        
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        LoggerManager._initialized = True
    
    def get_logger(self) -> logging.Logger:
        return self.logger


logger_manager = LoggerManager()
_logger = logger_manager.get_logger()


def log_info(message: str):
    _logger.info(message)

def log_error(message: str, exc_info: bool = True):
    _logger.error(message, exc_info=exc_info)

def log_warning(message: str):
    _logger.warning(message)

def log_exception(message: str, exc: Exception):
    _logger.exception(f"{message}: {str(exc)}")


# ============================================================================
# 5. 行業分類標準化
# ============================================================================
class IndustryClassifier:
    HSIS_MAPPING = {
        'bank': '金融業', 'banks': '金融業', '銀行': '金融業',
        'insurance': '金融業', '保險': '金融業',
        'securities': '金融業', '券商': '金融業',
        '投資': '金融業', 'financial': '金融業',
        'technology': '資訊科技業', '科技': '資訊科技業',
        'software': '資訊科技業', '軟件': '資訊科技業',
        'internet': '資訊科技業', '互聯網': '資訊科技業',
        'semiconductor': '資訊科技業', '半導體': '資訊科技業',
        'electronics': '資訊科技業', '電子': '資訊科技業',
        'healthcare': '醫療保健業', '醫療': '醫療保健業',
        'pharmaceutical': '醫療保健業', '藥業': '醫療保健業',
        'biotech': '醫療保健業', '生物科技': '醫療保健業',
        'consumer': '非必需性消費業', '消費': '非必需性消費業',
        'retail': '非必需性消費業', '零售': '非必需性消費業',
        'luxury': '非必需性消費業', '奢侈品': '非必需性消費業',
        'automobile': '非必需性消費業', '汽車': '非必需性消費業',
        'food': '必需性消費業', '食品': '必需性消費業',
        'beverage': '必需性消費業', '飲品': '必需性消費業',
        'agriculture': '必需性消費業', '農業': '必需性消費業',
        'utility': '公用事業', '公用事業': '公用事業',
        'power': '公用事業', '電力': '公用事業',
        'gas': '公用事業', '煤氣': '公用事業',
        'water': '公用事業', '水務': '公用事業',
        'telecom': '電訊業', '電訊': '電訊業',
        'transport': '運輸業', '運輸': '運輸業',
        'airline': '運輸業', '航空': '運輸業',
        'shipping': '運輸業', '航運': '運輸業',
        'logistics': '運輸業', '物流': '運輸業',
        'real estate': '房地產業', '地產': '房地產業',
        'property': '房地產業', 'reit': '房地產業',
        'conglomerate': '綜合企業', '綜合企業': '綜合企業',
        'diversified': '綜合企業',
        'materials': '原材料業', '原材料': '原材料業',
        'metal': '原材料業', '金屬': '原材料業',
        'mining': '原材料業', '礦業': '原材料業',
        'steel': '原材料業', '鋼鐵': '原材料業',
        'industrial': '工業', '工業': '工業',
        'manufacturing': '工業', '製造': '工業',
        'construction': '工業', '建築': '工業',
        'engineering': '工業', '工程': '工業',
        'energy': '能源業', '能源': '能源業',
        'oil': '能源業', '石油': '能源業',
        '天然氣': '能源業',
    }
    
    DEFAULT_INDUSTRY = '其他'
    
    @classmethod
    def classify(cls, text: str) -> str:
        if not text or not isinstance(text, str):
            return cls.DEFAULT_INDUSTRY
        
        text = text.lower().strip()
        
        for key, industry in cls.HSIS_MAPPING.items():
            if key in text:
                return industry
        
        return cls.DEFAULT_INDUSTRY


# ============================================================================
# 6. 快取機制
# ============================================================================
class CacheManager:
    @dataclass
    class CacheItem:
        key: str
        value: Any
        created_at: float
        accessed_at: float
        ttl_seconds: int
        hit_count: int = 0
        
        def is_expired(self, current_time: float) -> bool:
            if self.ttl_seconds <= 0:
                return False
            return (current_time - self.created_at) > self.ttl_seconds
    
    def __init__(self, max_size: int = 2000, default_ttl_hours: int = 24, 
                 max_memory_mb: float = 200.0):
        self.max_size = max_size
        self.default_ttl_seconds = default_ttl_hours * 3600
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        
        self._cache: OrderedDict[str, self.CacheItem] = OrderedDict()
        self._lock = threading.Lock()
        
        self._stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'evictions': 0,
            'expirations': 0
        }
        
        self._memory_usage = 0
        
        log_info(f"緩存初始化完成: max_size={max_size}, ttl_hours={default_ttl_hours}")
    
    def _estimate_size(self, value: Any) -> int:
        try:
            import sys
            return sys.getsizeof(value)
        except:
            return 1024
    
    def _evict_lru(self, current_time: float):
        while (len(self._cache) >= self.max_size or 
               self._memory_usage > self.max_memory_bytes):
            if not self._cache:
                break
            
            lru_key = None
            lru_access_time = float('inf')
            
            for key, item in self._cache.items():
                if item.accessed_at < lru_access_time:
                    lru_access_time = item.accessed_at
                    lru_key = key
            
            if lru_key:
                self._remove(lru_key, current_time)
                self._stats['evictions'] += 1
    
    def _remove(self, key: str, current_time: float):
        if key in self._cache:
            item = self._cache[key]
            self._memory_usage -= self._estimate_size(item.value)
            del self._cache[key]
    
    def _cleanup_expired(self, current_time: float):
        expired_keys = [key for key, item in self._cache.items() if item.is_expired(current_time)]
        for key in expired_keys:
            self._remove(key, current_time)
            self._stats['expirations'] += 1
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            current_time = time.time()
            self._cleanup_expired(current_time)
            
            if key in self._cache:
                item = self._cache[key]
                if item.is_expired(current_time):
                    self._remove(key, current_time)
                    self._stats['misses'] += 1
                    return None
                item.accessed_at = current_time
                item.hit_count += 1
                self._cache.move_to_end(key)
                self._stats['hits'] += 1
                return item.value
            
            self._stats['misses'] += 1
            return None
    
    def set(self, key: str, value: Any, ttl_hours: int = None):
        with self._lock:
            current_time = time.time()
            
            if key in self._cache:
                self._remove(key, current_time)
            
            ttl_seconds = (ttl_hours or self.default_ttl_seconds)
            
            item = self.CacheItem(
                key=key,
                value=value,
                created_at=current_time,
                accessed_at=current_time,
                ttl_seconds=ttl_seconds
            )
            
            value_size = self._estimate_size(value)
            while (self._memory_usage + value_size > self.max_memory_bytes and len(self._cache) > 0):
                self._evict_lru(current_time)
            
            self._cache[key] = item
            self._memory_usage += value_size
            self._evict_lru(current_time)
            self._stats['sets'] += 1
    
    def clear(self):
        with self._lock:
            self._cache.clear()
            self._memory_usage = 0
            log_info("緩存已清空")
    
    def get_stats(self) -> Dict:
        with self._lock:
            total = self._stats['hits'] + self._stats['misses']
            hit_rate = (self._stats['hits'] / total * 100) if total > 0 else 0
            
            return {
                'hits': self._stats['hits'],
                'misses': self._stats['misses'],
                'hit_rate': f"{hit_rate:.2f}%",
                'sets': self._stats['sets'],
                'evictions': self._stats['evictions'],
                'expirations': self._stats['expirations'],
                'items': len(self._cache),
                'memory_mb': round(self._memory_usage / 1024 / 1024, 2)
            }


# ============================================================================
# 7. 輸入驗證函數
# ============================================================================
def validate_stock_code(code: str) -> str:
    if not code:
        raise AppError(ErrorCode.INVALID_STOCK_CODE, details="股票代碼不能為空")
    
    numbers = re.sub(r'[^0-9]', '', str(code))
    
    if len(numbers) == 0:
        raise AppError(ErrorCode.INVALID_STOCK_CODE, details=f"股票代碼不包含數字: {code}")
    
    if len(numbers) <= 4:
        cleaned = numbers.zfill(4)
    elif len(numbers) == 5:
        cleaned = numbers
    else:
        cleaned = numbers[:5]
    
    code_int = int(cleaned)
    if code_int == 0 or code_int > 99999:
        raise AppError(ErrorCode.INVALID_STOCK_CODE, details=f"股票代碼超出範圍: {code}")
    
    return cleaned


def safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None or pd.isna(val):
            return default
        if isinstance(val, (pd.Series, pd.DataFrame)):
            if val.empty:
                return default
            val = val.iloc[0] if isinstance(val, pd.Series) else val.iloc[0, 0]
        return float(val)
    except:
        return default


# ============================================================================
# 8. 配置管理 (單例模式)
# ============================================================================
class ConfigManager:
    _instance = None
    
    DEFAULT_CONFIG = {
        'rate_limit_per_min': 120,
        'max_retries': 3,
        'data_retention_days': 365,
        'min_data_points': 30,
        'clean_data': True,
        'debug_mode': False,
        'log_level': 'INFO',
        'cache_enabled': True,
        'cache_ttl_hours': 24,
        'workers': 4,
        'timeout_seconds': 30,
        'batch_size': 1000,
        'async_concurrent': 10,
        'max_cache_size': 2000,
        'max_cache_memory_mb': 200.0,
        'default_initial_capital': 100000.0,
        'default_position_size': 0.1,
        'default_stop_loss': 0.05,
        'default_take_profit': 0.15,
        'auto_update_enabled': False,
        'auto_update_mode': 'scheduled',
        'auto_update_time': '12:00',
        'auto_update_interval_hours': 6,
        'auto_update_max_stocks': 676,
        'auto_update_outdated_days': 1,
        'auto_update_notify': False,
        'notification_enabled': True,
        'telegram_enabled': False,
        'telegram_bot_token': '',
        'telegram_chat_id': '',
        'telegram_parse_mode': 'HTML',
        'line_enabled': False,
        'line_access_token': '',
        'line_notify_token': '',
        'whatsapp_enabled': False,
        'whatsapp_account_sid': '',
        'whatsapp_auth_token': '',
        'whatsapp_from_number': '',
        'whatsapp_to_number': '',
        'email_enabled': False,
        'email_smtp_server': 'smtp.gmail.com',
        'email_smtp_port': 587,
        'email_sender': '',
        'email_password': '',
        'email_recipients': '',
        'email_use_tls': True,
        'webhook_enabled': False,
        'webhook_url': '',
        'webhook_method': 'POST',
        'webhook_headers': '',
        'default_min_5d_return': 5.0,
        'default_min_3d_return': 8.0,
        'default_volume_ratio': 1.5,
        'default_price_strength': 80.0,
        'default_max_rsi': 80,
        'db_busy_timeout': 60000,
        'db_pool_size': 10,
        'db_max_retries': 5,
        'db_retry_base_delay': 0.1,
        'db_retry_max_delay': 10.0,
    }
    
    def __new__(cls, config_file: str = "config.json"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init(config_file)
        return cls._instance
    
    def _init(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_and_validate_config()
    
    def _load_and_validate_config(self) -> Dict:
        config = self.DEFAULT_CONFIG.copy()
        
        try:
            config_path = Path(self.config_file)
            
            if config_path.exists():
                file_size = config_path.stat().st_size
                
                if file_size > 0:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if content:
                            loaded = json.loads(content)
                            config.update(loaded)
                            log_info(f"已載入配置文件: {self.config_file}")
            else:
                self._create_default_config()
                
        except json.JSONDecodeError as e:
            log_error(f"配置文件 JSON 解析錯誤: {e}")
            try:
                Path(self.config_file).unlink()
            except:
                pass
        except Exception as e:
            log_error(f"配置文件處理失敗: {e}")
        
        return config
    
    def _create_default_config(self):
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
            log_info(f"已創建預設配置文件: {self.config_file}")
        except Exception as e:
            log_error(f"創建配置文件失敗: {e}")
    
    def get(self, key: str, default=None):
        return self.config.get(key, default)
    
    def set(self, key: str, value):
        self.config[key] = value
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            log_error(f"保存配置失敗: {e}")


# ============================================================================
# 9. 技術指標計算
# ============================================================================
class TechnicalIndicators:
    @staticmethod
    def calculate_all(df: pd.DataFrame) -> Optional[Dict]:
        try:
            if df is None or len(df) < 30:
                return None
            
            close = pd.to_numeric(df['Close'], errors='coerce').fillna(0)
            high = pd.to_numeric(df['High'], errors='coerce').fillna(0)
            low = pd.to_numeric(df['Low'], errors='coerce').fillna(0)
            volume = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
            
            if close.max() == 0 or len(close) < 30:
                return None
            
            ma5 = close.rolling(5).mean().iloc[-1] if len(close) >= 5 else close.iloc[-1]
            ma10 = close.rolling(10).mean().iloc[-1] if len(close) >= 10 else close.iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else close.iloc[-1]
            ma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else close.iloc[-1]
            ma100 = close.rolling(100).mean().iloc[-1] if len(close) >= 100 else close.iloc[-1]
            ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else close.iloc[-1]
            
            rsi = TechnicalIndicators._calculate_rsi(close, 14)
            macd_result = TechnicalIndicators._calculate_macd(close)
            bb_result = TechnicalIndicators._calculate_bollinger_bands(close)
            atr = TechnicalIndicators._calculate_atr(high, low, close)
            atr_percent = (atr / close.iloc[-1] * 100) if close.iloc[-1] != 0 else 0
            stoch_k = TechnicalIndicators._calculate_stochastic_k(high, low, close)
            stoch_d = TechnicalIndicators._calculate_stochastic_d(high, low, close)
            vol_ratio = TechnicalIndicators._calculate_volume_ratio(volume)
            roc_5 = TechnicalIndicators._calculate_roc(close, 5)
            ret_5d = roc_5
            ret_3d = TechnicalIndicators._calculate_roc(close, 3)
            ret_20d = TechnicalIndicators._calculate_roc(close, 20)
            price_strength = TechnicalIndicators._calculate_price_strength(close, high, low)
            
            return {
                'close': float(close.iloc[-1]),
                'ma5': float(ma5), 'ma10': float(ma10), 'ma20': float(ma20),
                'ma50': float(ma50), 'ma100': float(ma100), 'ma200': float(ma200),
                'rsi': float(rsi),
                'macd_line': float(macd_result['macd_line']),
                'signal_line': float(macd_result['signal_line']),
                'histogram': float(macd_result['histogram']),
                'macd_trend': macd_result['macd_trend'],
                'bb_upper': float(bb_result[0]), 'bb_middle': float(bb_result[1]), 'bb_lower': float(bb_result[2]),
                'atr': float(atr), 'atr_percent': float(atr_percent),
                'stoch_k': float(stoch_k), 'stoch_d': float(stoch_d),
                'volume_ratio': float(vol_ratio),
                'roc_5': float(roc_5),
                'ret_5d': float(ret_5d), 'ret_3d': float(ret_3d), 'ret_20d': float(ret_20d),
                'price_strength': float(price_strength),
            }
            
        except Exception as e:
            log_error(f"計算技術指標失敗: {e}")
            return None
    
    @staticmethod
    def _calculate_rsi(close: pd.Series, period: int = 14) -> float:
        try:
            if len(close) < period + 1:
                return 50.0
            delta = close.diff()
            gain = delta.where(delta > 0, 0)
            loss = (-delta).where(delta < 0, 0)
            avg_gain = gain.rolling(period).mean()
            avg_loss = loss.rolling(period).mean()
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50.0
        except:
            return 50.0
    
    @staticmethod
    def _calculate_macd(close: pd.Series, fast_period: int = 12, 
                        slow_period: int = 26, signal_period: int = 9) -> Dict[str, float]:
        try:
            fast_ema = close.ewm(span=fast_period, adjust=False).mean()
            slow_ema = close.ewm(span=slow_period, adjust=False).mean()
            macd_line = fast_ema - slow_ema
            signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
            histogram = macd_line - signal_line
            return {
                'macd_line': float(macd_line.iloc[-1]),
                'signal_line': float(signal_line.iloc[-1]),
                'histogram': float(histogram.iloc[-1]),
                'macd_trend': 'bullish' if macd_line.iloc[-1] > signal_line.iloc[-1] else 'bearish'
            }
        except:
            return {'macd_line': 0, 'signal_line': 0, 'histogram': 0, 'macd_trend': 'neutral'}
    
    @staticmethod
    def _calculate_bollinger_bands(close: pd.Series, period: int = 20) -> Tuple[float, float, float]:
        try:
            middle = close.rolling(period).mean()
            std = close.rolling(period).std()
            upper = middle + (std * 2)
            lower = middle - (std * 2)
            return (float(upper.iloc[-1]), float(middle.iloc[-1]), float(lower.iloc[-1]))
        except:
            return (0, 0, 0)
    
    @staticmethod
    def _calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        try:
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            return float(tr.rolling(period).mean().iloc[-1])
        except:
            return 0
    
    @staticmethod
    def _calculate_stochastic_k(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        try:
            lowest_low = low.rolling(period).min()
            highest_high = high.rolling(period).max()
            return float(100 * (close - lowest_low) / (highest_high - lowest_low).iloc[-1])
        except:
            return 50
    
    @staticmethod
    def _calculate_stochastic_d(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        try:
            stoch_k = TechnicalIndicators._calculate_stochastic_k(high, low, close, period)
            return float(pd.Series([stoch_k]).rolling(3).mean().iloc[-1])
        except:
            return 50
    
    @staticmethod
    def _calculate_volume_ratio(volume: pd.Series, short_period: int = 5) -> float:
        try:
            if len(volume) < short_period:
                return 1.0
            short_ma = volume.rolling(short_period).mean().iloc[-1]
            if short_ma <= 0:
                return 1.0
            return float(volume.iloc[-1] / short_ma)
        except:
            return 1.0
    
    @staticmethod
    def _calculate_roc(close: pd.Series, period: int) -> float:
        try:
            if len(close) < period + 1:
                return 0
            return float((close.iloc[-1] - close.iloc[-period-1]) / close.iloc[-period-1] * 100)
        except:
            return 0
    
    @staticmethod
    def _calculate_price_strength(close: pd.Series, high: pd.Series, low: pd.Series, period: int = 20) -> float:
        try:
            if len(close) < period:
                return 50.0
            recent_high = high.iloc[-period:].max()
            recent_low = low.iloc[-period:].min()
            if recent_high == recent_low:
                return 50
            return float((close.iloc[-1] - recent_low) / (recent_high - recent_low) * 100)
        except:
            return 50.0


# ============================================================================
# 10. 股票清單載入
# ============================================================================
def load_stock_list(json_file: str = None) -> Optional[pd.DataFrame]:
    default_files = [
        "hk_stocks_combined_4d.json",
        "hk_stocks_combined.json",
        "hk_stocks.json"
    ]
    
    current_dir = Path(__file__).parent
    
    if json_file:
        file_path = Path(json_file)
        if not file_path.is_absolute():
            file_path = current_dir / file_path
        file_list = [file_path]
    else:
        file_list = [current_dir / f for f in default_files]
    
    for file_path in file_list:
        if not file_path.exists():
            continue
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                raise AppError(ErrorCode.INVALID_DATA_FORMAT, details="JSON 格式錯誤")
            
            df = pd.DataFrame(data)
            
            if 'code' not in df.columns or 'name' not in df.columns:
                raise AppError(ErrorCode.INVALID_DATA_FORMAT, details="缺少必要欄位")
            
            df['code'] = df['code'].apply(validate_stock_code)
            df['ticker'] = df['code'] + '.HK'
            df['name'] = df['name'].astype(str).str.strip()
            df['category'] = df.get('category', '未分類').astype(str).str.strip()
            df['industry'] = df['category'].apply(IndustryClassifier.classify)
            
            df = df.drop_duplicates(subset=['code']).sort_values('code').reset_index(drop=True)
            
            log_info(f"載入 {file_path.name}: {len(df)} 檔股票")
            
            return df[['code', 'name', 'ticker', 'category', 'industry']]
            
        except Exception as e:
            log_error(f"載入 {file_path} 失敗: {e}")
            continue
    
    return None


# ============================================================================
# 11. Yahoo Finance 下載器
# ============================================================================
class YahooFinanceDownloader:
    def __init__(self, rate_limit_per_min: int = None, max_retries: int = None,
                 timeout_seconds: int = None):
        global config
        
        if config is None:
            config = ConfigManager()
        
        self.rate_limit_per_min = rate_limit_per_min or config.get('rate_limit_per_min', 60)
        self.max_retries = max_retries or config.get('max_retries', 3)
        self.timeout_seconds = timeout_seconds or config.get('timeout_seconds', 30)
        
        self.request_times = deque(maxlen=self.rate_limit_per_min)
        self.lock = threading.Lock()
        self.problematic_tickers = set()
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'empty_data': 0
        }
        
        log_info(f"同步下載器初始化: 速率={self.rate_limit_per_min}/分, 超時={self.timeout_seconds}秒")
    
    def _wait_if_needed(self):
        with self.lock:
            now = time.time()
            
            while self.request_times and now - self.request_times[0] > 60:
                self.request_times.popleft()
            
            if len(self.request_times) >= self.rate_limit_per_min:
                sleep_time = 60 - (now - self.request_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    self.request_times.clear()
            
            self.request_times.append(time.time())
    
    def download(self, ticker: str, period: str = "1y") -> Optional[pd.DataFrame]:
        numbers = re.sub(r'[^0-9]', '', str(ticker))
        if len(numbers) == 0:
            log_warning(f"無效的股票報價: {ticker}")
            return None
        
        if len(numbers) == 5:
            cleaned = f"{numbers}.HK"
        else:
            cleaned = f"{numbers.zfill(4)}.HK"
        
        if cleaned in self.problematic_tickers:
            return None
        
        for attempt in range(self.max_retries):
            try:
                self._wait_if_needed()
                self.stats['total_requests'] += 1
                
                stock = yf.Ticker(cleaned)
                df = stock.history(period=period, timeout=self.timeout_seconds, auto_adjust=True)
                
                if df is None or df.empty or len(df) < 5:
                    self.stats['empty_data'] += 1
                    if attempt < self.max_retries - 1:
                        time.sleep((attempt + 1) * 2)
                        continue
                    return None
                
                df = df.reset_index()
                
                required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = 0.0
                
                numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                if 'Date' in df.columns:
                    try:
                        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dropna()
                        if df.empty:
                            return None
                        df.set_index('Date', inplace=True)
                    except:
                        df = df.drop('Date', axis=1)
                
                self.stats['successful_requests'] += 1
                return df
                
            except Exception as e:
                error_msg = str(e)
                
                if "arg must be a list" in error_msg:
                    self.problematic_tickers.add(cleaned)
                    self.stats['failed_requests'] += 1
                    return None
                
                if attempt < self.max_retries - 1:
                    time.sleep((attempt + 1) * 3)
                
                self.stats['failed_requests'] += 1
        
        return None
    
    def get_stats(self) -> Dict:
        total = self.stats['total_requests']
        success_rate = (self.stats['successful_requests'] / total * 100) if total > 0 else 0
        
        return {
            'total_requests': total,
            'successful_requests': self.stats['successful_requests'],
            'failed_requests': self.stats['failed_requests'],
            'empty_data': self.stats['empty_data'],
            'success_rate': f"{success_rate:.2f}%"
        }


# ============================================================================
# 12. 數據庫管理器 (線程安全 + 連接池 + 並發支持)
# ============================================================================
class DatabaseConnectionPool:
    def __init__(self, db_path: str, max_connections: int = 10, 
                 busy_timeout: int = 60000):
        self.db_path = db_path
        self.max_connections = max_connections
        self.busy_timeout = busy_timeout
        self._pool: deque = deque(maxlen=max_connections)
        self._in_use: Dict[int, sqlite3.Connection] = {}
        self._lock = threading.Lock()
        self._total_created = 0
        self._total_reused = 0
        
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        log_info(f"連接池初始化: {db_path}, 最大連接數={max_connections}, busy_timeout={busy_timeout}ms")
    
    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=30,
            check_same_thread=False,
            isolation_level=None
        )
        
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout}")
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA cache_size=-64000')
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('PRAGMA mmap_size=268435456')
        
        self._total_created += 1
        return conn
    
    @contextmanager
    def get_connection(self):
        conn = None
        thread_id = threading.current_thread().ident
        
        try:
            with self._lock:
                while self._pool:
                    candidate = self._pool.pop()
                    try:
                        candidate.execute("SELECT 1")
                        conn = candidate
                        self._total_reused += 1
                        break
                    except Exception:
                        try:
                            candidate.close()
                        except:
                            pass
            
            if conn is None:
                conn = self._create_connection()
            
            with self._lock:
                self._in_use[thread_id] = conn
            
            yield conn
            
        finally:
            if conn is not None:
                with self._lock:
                    if thread_id in self._in_use:
                        del self._in_use[thread_id]
                    if len(self._pool) < self.max_connections:
                        try:
                            conn.execute("SELECT 1")
                            self._pool.append(conn)
                        except Exception:
                            try:
                                conn.close()
                            except:
                                pass
                    else:
                        try:
                            conn.close()
                        except:
                            pass
    
    def get_stats(self) -> Dict:
        with self._lock:
            return {
                'pool_size': len(self._pool),
                'in_use': len(self._in_use),
                'total_created': self._total_created,
                'total_reused': self._total_reused,
                'reuse_rate': f"{(self._total_reused / self._total_created * 100):.1f}%" if self._total_created > 0 else "0%"
            }
    
    def close_all(self):
        with self._lock:
            while self._pool:
                try:
                    self._pool.pop().close()
                except:
                    pass
            
            for conn in self._in_use.values():
                try:
                    conn.close()
                except:
                    pass
            
            self._in_use.clear()
            log_info("連接池已關閉所有連接")


class DatabaseManager:
    _instance = None
    _initialized = False
    
    def __new__(cls, db_path: str = "hk_stocks.db"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init(db_path)
        return cls._instance
    
    def _init(self, db_path: str):
        global config
        
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_flag = False
        
        if config is None:
            config = ConfigManager()
        
        self._pool = DatabaseConnectionPool(
            db_path=db_path,
            max_connections=config.get('db_pool_size', 10),
            busy_timeout=config.get('db_busy_timeout', 60000)
        )
        
        self._max_retries = config.get('db_max_retries', 5)
        self._retry_base_delay = config.get('db_retry_base_delay', 0.1)
        self._retry_max_delay = config.get('db_retry_max_delay', 10.0)
        
        self._init_database()
        log_info(f"數據庫管理器初始化完成: {db_path}")
    
    def _init_database(self):
        with self._lock:
            if self._init_flag:
                return
            
            try:
                with self._pool.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS stock_info (
                            code TEXT PRIMARY KEY, name TEXT NOT NULL, category TEXT,
                            industry TEXT DEFAULT '其他', ticker TEXT UNIQUE, last_updated TIMESTAMP
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS stock_price (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, date DATE,
                            open REAL, high REAL, low REAL, close REAL, volume INTEGER, update_batch INTEGER,
                            FOREIGN KEY (code) REFERENCES stock_info (code), UNIQUE(code, date)
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS stock_indicators (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, date DATE, close REAL,
                            ma5 REAL, ma10 REAL, ma20 REAL, ma50 REAL, ma100 REAL, ma200 REAL,
                            rsi REAL, macd_line REAL, signal_line REAL, histogram REAL,
                            bb_upper REAL, bb_middle REAL, bb_lower REAL,
                            atr REAL, atr_percent REAL, stoch_k REAL, stoch_d REAL,
                            roc_5 REAL, volume_ratio REAL,
                            ret_5d REAL, ret_3d REAL, ret_20d REAL,
                            price_strength REAL,
                            FOREIGN KEY (code) REFERENCES stock_info (code), UNIQUE(code, date)
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS update_history (
                            batch_id INTEGER PRIMARY KEY AUTOINCREMENT, start_time TIMESTAMP, end_time TIMESTAMP,
                            total_stocks INTEGER, updated_stocks INTEGER, failed_stocks INTEGER,
                            skipped_stocks INTEGER, status TEXT
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS backtest_results (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, strategy_name TEXT, ticker TEXT,
                            start_date DATE, end_date DATE, initial_capital REAL, final_capital REAL,
                            total_return REAL, annual_return REAL, max_drawdown REAL, sharpe_ratio REAL,
                            win_rate REAL, num_trades INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS alerts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, condition_type TEXT,
                            condition_value REAL, is_triggered BOOLEAN DEFAULT 0,
                            last_triggered TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS notification_config (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            platform TEXT NOT NULL,
                            config_json TEXT NOT NULL,
                            is_enabled BOOLEAN DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS notification_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            alert_id INTEGER,
                            platform TEXT NOT NULL,
                            message TEXT,
                            status TEXT,
                            error_message TEXT,
                            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (alert_id) REFERENCES alerts (id)
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS background_tasks (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            task_name TEXT NOT NULL,
                            task_type TEXT,
                            status TEXT DEFAULT 'pending',
                            last_run TIMESTAMP,
                            next_run TIMESTAMP,
                            config_json TEXT,
                            result_json TEXT,
                            error_message TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS schema_version (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, version INTEGER NOT NULL,
                            migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_code ON stock_price(code)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_indicators_code ON stock_indicators(code)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bg_task_name ON background_tasks(task_name)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_alerts_id ON alerts(id)')
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_notification_config_platform ON notification_config(platform)')
                    
                    self._init_flag = True
                    log_info("數據庫結構初始化完成 (v9)")
                    
            except Exception as e:
                log_error(f"數據庫初始化失敗: {e}")
                raise
    
    def _execute_with_retry(self, query: str, params: tuple = None) -> Any:
        last_exception = None
        
        for attempt in range(self._max_retries):
            try:
                with self._pool.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    if query.strip().upper().startswith('SELECT'):
                        return cursor.fetchall()
                    else:
                        return True
                        
            except sqlite3.OperationalError as e:
                error_msg = str(e).lower()
                last_exception = e
                
                if 'locked' in error_msg or 'busy' in error_msg:
                    wait_time = min(
                        self._retry_base_delay * (2 ** attempt),
                        self._retry_max_delay
                    )
                    log_warning(f"數據庫被鎖定，等待 {wait_time:.2f} 秒後重試 ({attempt + 1}/{self._max_retries})")
                    time.sleep(wait_time)
                else:
                    raise
            
            except Exception as e:
                log_error(f"數據庫操作失敗: {e}")
                raise
        
        log_error(f"數據庫操作失敗，已重試 {self._max_retries} 次")
        raise AppError(ErrorCode.DB_LOCKED, details=f"多次重試後仍被鎖定: {str(last_exception)}")
    
    def save_stock_info_batch(self, stocks_df: pd.DataFrame):
        for _, row in stocks_df.iterrows():
            code = validate_stock_code(row['code'])
            name = str(row['name']).strip()
            industry = str(row.get('industry', '其他')).strip()
            ticker = f"{code}.HK"
            
            self._execute_with_retry('''
                INSERT OR REPLACE INTO stock_info (code, name, category, industry, ticker, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (code, name, str(row.get('category', 'N/A')), industry, ticker, datetime.now()))
    
    def needs_update(self, code: str, days_threshold: int = 1) -> bool:
        result = self._execute_with_retry(
            'SELECT MAX(date) FROM stock_price WHERE code = ?', (code,)
        )
        
        last_date = result[0][0] if result and result[0] else None
        
        if last_date is None:
            return True
        
        try:
            last_update_date = datetime.strptime(str(last_date), '%Y-%m-%d').date()
            return (datetime.now().date() - last_update_date).days >= days_threshold
        except:
            return True
    
    def get_outdated_stocks(self, stocks_df: pd.DataFrame, days_threshold: int = 1) -> List[Dict]:
        result = self._execute_with_retry('''
            SELECT code, MAX(date) as last_date 
            FROM stock_price 
            GROUP BY code
        ''')
        
        db_last_dates = {row[0]: row[1] for row in result}
        
        outdated = []
        for _, row in stocks_df.iterrows():
            code = row['code']
            last_date = db_last_dates.get(code)
            
            needs_update = False
            if last_date is None:
                needs_update = True
            else:
                try:
                    last_update_date = datetime.strptime(str(last_date), '%Y-%m-%d').date()
                    if (datetime.now().date() - last_update_date).days >= days_threshold:
                        needs_update = True
                except:
                    needs_update = True
            
            if needs_update:
                outdated.append({
                    'code': code,
                    'name': row['name'],
                    'ticker': row['ticker'],
                    'industry': row.get('industry', '其他'),
                    'last_update': last_date
                })
        
        return outdated
    
    def save_price_and_indicators(self, code: str, price_data: pd.DataFrame, 
                                  indicators: Dict, batch_id: int) -> bool:
        try:
            with self._pool.get_connection() as conn:
                try:
                    cursor = conn.cursor()
                    
                    for date, row in price_data.iterrows():
                        cursor.execute('''
                            INSERT OR REPLACE INTO stock_price 
                            (code, date, open, high, low, close, volume, update_batch)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            code,
                            date.date() if hasattr(date, 'date') else date,
                            safe_float(row.get('Open')),
                            safe_float(row.get('High')),
                            safe_float(row.get('Low')),
                            safe_float(row.get('Close')),
                            int(row.get('Volume', 0)),
                            batch_id
                        ))
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO stock_indicators 
                        (code, date, close, ma5, ma10, ma20, ma50, ma100, ma200,
                         rsi, macd_line, signal_line, histogram,
                         bb_upper, bb_middle, bb_lower,
                         atr, atr_percent, stoch_k, stoch_d,
                         roc_5, volume_ratio,
                         ret_5d, ret_3d, ret_20d,
                         price_strength)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        code,
                        datetime.now().date(),
                        safe_float(indicators.get('close')),
                        safe_float(indicators.get('ma5')),
                        safe_float(indicators.get('ma10')),
                        safe_float(indicators.get('ma20')),
                        safe_float(indicators.get('ma50')),
                        safe_float(indicators.get('ma100')),
                        safe_float(indicators.get('ma200')),
                        safe_float(indicators.get('rsi')),
                        safe_float(indicators.get('macd_line')),
                        safe_float(indicators.get('signal_line')),
                        safe_float(indicators.get('histogram')),
                        safe_float(indicators.get('bb_upper')),
                        safe_float(indicators.get('bb_middle')),
                        safe_float(indicators.get('bb_lower')),
                        safe_float(indicators.get('atr')),
                        safe_float(indicators.get('atr_percent')),
                        safe_float(indicators.get('stoch_k')),
                        safe_float(indicators.get('stoch_d')),
                        safe_float(indicators.get('roc_5')),
                        safe_float(indicators.get('volume_ratio')),
                        safe_float(indicators.get('ret_5d')),
                        safe_float(indicators.get('ret_3d')),
                        safe_float(indicators.get('ret_20d')),
                        safe_float(indicators.get('price_strength')),
                    ))
                    
                    conn.commit()
                    return True
                    
                except Exception as e:
                    conn.rollback()
                    log_error(f"保存數據失敗 {code}: {e}")
                    return False
                    
        except AppError:
            raise
        except Exception as e:
            log_error(f"保存數據異常 {code}: {e}")
            return False
    
    def start_batch(self, total_stocks: int) -> int:
        result = self._execute_with_retry('''
            INSERT INTO update_history (start_time, total_stocks, status)
            VALUES (?, ?, ?)
        ''', (datetime.now(), total_stocks, 'running'))
        
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT MAX(batch_id) FROM update_history')
            return cursor.fetchone()[0]
    
    def end_batch(self, batch_id: int, updated: int, failed: int, 
                  skipped: int, status: str = 'completed'):
        self._execute_with_retry('''
            UPDATE update_history 
            SET end_time = ?, updated_stocks = ?, failed_stocks = ?, skipped_stocks = ?, status = ?
            WHERE batch_id = ?
        ''', (datetime.now(), updated, failed, skipped, status, batch_id))
    
    def get_all_indicators(self) -> pd.DataFrame:
        with self._pool.get_connection() as conn:
            return pd.read_sql_query('''
                SELECT i.*, s.name, s.category, s.industry
                FROM stock_indicators i
                LEFT JOIN stock_info s ON i.code = s.code
                WHERE i.date = (SELECT MAX(date) FROM stock_indicators WHERE code = i.code)
            ''', conn)
    
    def save_backtest_result(self, result: Dict):
        self._execute_with_retry('''
            INSERT INTO backtest_results 
            (strategy_name, ticker, start_date, end_date, initial_capital, final_capital,
             total_return, annual_return, max_drawdown, sharpe_ratio, win_rate, num_trades)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            result.get('strategy_name'), result.get('ticker'),
            result.get('start_date'), result.get('end_date'),
            result.get('initial_capital'), result.get('final_capital'),
            result.get('total_return'), result.get('annual_return'),
            result.get('max_drawdown'), result.get('sharpe_ratio'),
            result.get('win_rate'), result.get('num_trades')
        ))
    
    def get_backtest_results(self) -> pd.DataFrame:
        with self._pool.get_connection() as conn:
            return pd.read_sql_query('''
                SELECT * FROM backtest_results ORDER BY created_at DESC LIMIT 100
            ''', conn)
    
    def save_alert(self, alert: Dict) -> int:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO alerts (ticker, condition_type, condition_value)
                VALUES (?, ?, ?)
            ''', (alert.get('ticker'), alert.get('condition_type'), alert.get('condition_value')))
            conn.commit()
            return cursor.lastrowid
    
    def get_alerts(self) -> pd.DataFrame:
        with self._pool.get_connection() as conn:
            return pd.read_sql_query('SELECT * FROM alerts ORDER BY created_at DESC', conn)
    
    def delete_alert(self, alert_id: int) -> bool:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM alerts WHERE id = ?', (alert_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def delete_all_alerts(self) -> int:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM alerts')
            conn.commit()
            return cursor.rowcount
    
    def save_notification_config(self, platform: str, config: Dict, is_enabled: bool = False) -> int:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            config_json = json.dumps(config, ensure_ascii=False)
            
            cursor.execute('''
                INSERT OR REPLACE INTO notification_config (platform, config_json, is_enabled, updated_at)
                VALUES (?, ?, ?, ?)
            ''', (platform, config_json, is_enabled, datetime.now()))
            
            conn.commit()
            return cursor.lastrowid
    
    def get_all_notification_configs(self) -> List[Dict]:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT platform, config_json, is_enabled FROM notification_config ORDER BY platform
            ''')
            results = cursor.fetchall()
            
            configs = []
            for row in results:
                configs.append({
                    'platform': row[0],
                    'config': json.loads(row[1]),
                    'is_enabled': bool(row[2])
                })
            
            return configs
    
    def save_background_task(self, task_name: str, task_type: str, config: Dict = None, 
                             next_run: datetime = None) -> int:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            config_json = json.dumps(config or {}, ensure_ascii=False)
            
            cursor.execute('''
                INSERT INTO background_tasks (task_name, task_type, config_json, next_run, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (task_name, task_type, config_json, next_run, 'pending'))
            
            conn.commit()
            return cursor.lastrowid
    
    def update_background_task(self, task_id: int, status: str = None, result: Dict = None,
                              error_message: str = None, next_run: datetime = None):
        with self._pool.get_connection() as conn:
            result_json = json.dumps(result, ensure_ascii=False) if result else None
            
            if task_id == -1:
                if status:
                    conn.execute('''
                        UPDATE background_tasks 
                        SET status = ?, result_json = ?, error_message = ?, last_run = ?, next_run = ?
                        WHERE id = (SELECT MAX(id) FROM background_tasks)
                    ''', (status, result_json, error_message, datetime.now(), next_run))
                else:
                    conn.execute('''
                        UPDATE background_tasks 
                        SET result_json = ?, error_message = ?, last_run = ?, next_run = ?
                        WHERE id = (SELECT MAX(id) FROM background_tasks)
                    ''', (result_json, error_message, datetime.now(), next_run))
            else:
                if status:
                    conn.execute('''
                        UPDATE background_tasks 
                        SET status = ?, result_json = ?, error_message = ?, last_run = ?, next_run = ?
                        WHERE id = ?
                    ''', (status, result_json, error_message, datetime.now(), next_run, task_id))
                else:
                    conn.execute('''
                        UPDATE background_tasks 
                        SET result_json = ?, error_message = ?, last_run = ?, next_run = ?
                        WHERE id = ?
                    ''', (result_json, error_message, datetime.now(), next_run, task_id))
            
            conn.commit()
    
    def get_background_task(self, task_name: str) -> Optional[Dict]:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, task_name, task_type, status, last_run, next_run, config_json, result_json, error_message
                FROM background_tasks 
                WHERE task_name = ?
                ORDER BY id DESC
                LIMIT 1
            ''', (task_name,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'id': result[0],
                    'task_name': result[1],
                    'task_type': result[2],
                    'status': result[3],
                    'last_run': result[4],
                    'next_run': result[5],
                    'config': json.loads(result[6]) if result[6] else {},
                    'result': json.loads(result[7]) if result[7] else None,
                    'error_message': result[8]
                }
            return None
    
    def get_all_background_tasks(self, limit: int = 50) -> pd.DataFrame:
        with self._pool.get_connection() as conn:
            return pd.read_sql_query(f'''
                SELECT id, task_name, task_type, status, last_run, next_run, error_message, created_at
                FROM background_tasks 
                ORDER BY created_at DESC
                LIMIT {limit}
            ''', conn)
    
    def clear_background_tasks(self) -> int:
        with self._pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM background_tasks')
            conn.commit()
            return cursor.rowcount
    
    def get_pool_stats(self) -> Dict:
        return self._pool.get_stats()


# ============================================================================
# 13. 即時推送系統
# ============================================================================

@dataclass
class NotificationMessage:
    title: str
    body: str
    ticker: str = ""
    alert_type: str = ""
    alert_value: float = 0.0
    current_price: float = 0.0
    indicators: Dict = field(default_factory=dict)
    priority: str = "normal"
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class NotificationProvider(ABC):
    def __init__(self, name: str, config: Dict = None):
        self.name = name
        self.config = config or {}
        self.is_enabled = False
        self.stats = {
            'sent': 0,
            'failed': 0,
            'last_sent': None
        }
    
    @abstractmethod
    def send(self, message: NotificationMessage) -> Tuple[bool, str]:
        pass
    
    @abstractmethod
    def test_connection(self) -> Tuple[bool, str]:
        pass


class TelegramProvider(NotificationProvider):
    def __init__(self, config: Dict = None):
        super().__init__("Telegram", config)
        self.bot_token = self.config.get('bot_token', '')
        self.chat_id = self.config.get('chat_id', '')
        self.parse_mode = self.config.get('parse_mode', 'HTML')
        self.is_enabled = bool(self.bot_token and self.chat_id)
        
        if self.is_enabled:
            self.api_url = f"https://api.telegram.org/bot{self.bot_token}"
    
    def test_connection(self) -> Tuple[bool, str]:
        if not self.bot_token:
            return False, "缺少 bot_token"
        if not self.chat_id:
            return False, "缺少 chat_id"
        
        try:
            response = requests.get(f"{self.api_url}/getMe", timeout=10)
            data = response.json()
            
            if data.get('ok'):
                bot_name = data['result'].get('username', 'Unknown')
                return True, f"連接成功！機器人: @{bot_name}"
            else:
                return False, f"連接失敗: {data.get('description', '未知錯誤')}"
        except Exception as e:
            return False, f"連接錯誤: {str(e)}"
    
    def send(self, message: NotificationMessage) -> Tuple[bool, str]:
        if not self.is_enabled:
            return False, "Telegram 未啟用"
        
        try:
            content = message.body if self.parse_mode == 'HTML' else f"{message.title}\n{message.body}"
            
            payload = {
                'chat_id': self.chat_id,
                'text': content,
                'parse_mode': self.parse_mode
            }
            
            response = requests.post(
                f"{self.api_url}/sendMessage",
                json=payload,
                timeout=30
            )
            
            data = response.json()
            
            if data.get('ok'):
                self.stats['sent'] += 1
                self.stats['last_sent'] = datetime.now()
                return True, "消息已發送"
            else:
                self.stats['failed'] += 1
                return False, f"發送失敗: {data.get('description', '未知錯誤')}"
                
        except Exception as e:
            self.stats['failed'] += 1
            log_error(f"Telegram 發送錯誤: {e}")
            return False, str(e)


class LineProvider(NotificationProvider):
    def __init__(self, config: Dict = None):
        super().__init__("LINE", config)
        self.access_token = self.config.get('access_token', '')
        self.notify_token = self.config.get('notify_token', '')
        self.is_enabled = bool(self.access_token) or bool(self.notify_token)
    
    def test_connection(self) -> Tuple[bool, str]:
        if not self.access_token and not self.notify_token:
            return False, "缺少 access_token 或 notify_token"
        
        try:
            headers = {'Authorization': f'Bearer {self.access_token or self.notify_token}'}
            response = requests.get("https://api.line.me/v2/bot/info", headers=headers, timeout=10)
            
            if response.status_code == 200:
                return True, "LINE 連接成功！"
            else:
                return False, f"連接失敗: HTTP {response.status_code}"
                
        except Exception as e:
            return False, f"連接錯誤: {str(e)}"
    
    def send(self, message: NotificationMessage) -> Tuple[bool, str]:
        if not self.is_enabled:
            return False, "LINE 未啟用"
        
        try:
            content = f"{message.title}\n{message.body}"
            
            headers = {
                'Authorization': f'Bearer {self.access_token or self.notify_token}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            payload = {'message': f"\n{content}"}
            
            response = requests.post(
                "https://notify-api.line.me/api/notify",
                headers=headers,
                data=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                self.stats['sent'] += 1
                self.stats['last_sent'] = datetime.now()
                return True, "消息已發送"
            else:
                self.stats['failed'] += 1
                return False, f"發送失敗: HTTP {response.status_code}"
                
        except Exception as e:
            self.stats['failed'] += 1
            log_error(f"LINE 發送錯誤: {e}")
            return False, str(e)


class WhatsAppProvider(NotificationProvider):
    def __init__(self, config: Dict = None):
        super().__init__("WhatsApp", config)
        self.account_sid = self.config.get('account_sid', '')
        self.auth_token = self.config.get('auth_token', '')
        self.from_number = self.config.get('from_number', '')
        self.to_number = self.config.get('to_number', '')
        self.is_enabled = bool(self.account_sid and self.auth_token and self.from_number)
        
        if self.is_enabled:
            self.api_url = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}/Messages.json"
    
    def test_connection(self) -> Tuple[bool, str]:
        if not self.account_sid or not self.auth_token:
            return False, "缺少 Account SID 或 Auth Token"
        
        try:
            response = requests.get(
                f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}.json",
                auth=(self.account_sid, self.auth_token),
                timeout=10
            )
            
            if response.status_code == 200:
                return True, "WhatsApp 連接成功！"
            else:
                return False, f"連接失敗: HTTP {response.status_code}"
                
        except Exception as e:
            return False, f"連接錯誤: {str(e)}"
    
    def send(self, message: NotificationMessage) -> Tuple[bool, str]:
        if not self.is_enabled:
            return False, "WhatsApp 未啟用"
        
        try:
            import re
            content = re.sub(r'[^\x00-\x7F]+', '', f"{message.title}\n{message.body}")
            
            payload = {
                'From': f"whatsapp:{self.from_number}",
                'To': f"whatsapp:{self.to_number}",
                'Body': content
            }
            
            response = requests.post(
                self.api_url,
                auth=(self.account_sid, self.auth_token),
                data=payload,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                self.stats['sent'] += 1
                self.stats['last_sent'] = datetime.now()
                return True, "消息已發送"
            else:
                self.stats['failed'] += 1
                return False, f"發送失敗"
                
        except Exception as e:
            self.stats['failed'] += 1
            log_error(f"WhatsApp 發送錯誤: {e}")
            return False, str(e)


class EmailProvider(NotificationProvider):
    def __init__(self, config: Dict = None):
        super().__init__("Email", config)
        self.smtp_server = self.config.get('smtp_server', 'smtp.gmail.com')
        self.smtp_port = self.config.get('smtp_port', 587)
        self.sender = self.config.get('sender', '')
        self.password = self.config.get('password', '')
        self.recipients = self.config.get('recipients', '')
        self.use_tls = self.config.get('use_tls', True)
        self.is_enabled = bool(self.sender and self.password and self.recipients)
    
    def test_connection(self) -> Tuple[bool, str]:
        if not self.sender or not self.password:
            return False, "缺少發送者郵箱或密碼"
        
        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10)
            
            if self.use_tls:
                server.starttls()
            
            server.login(self.sender, self.password)
            server.quit()
            
            return True, "郵箱連接成功！"
            
        except smtplib.SMTPAuthenticationError:
            return False, "認證失敗，請檢查用戶名和密碼"
        except Exception as e:
            return False, f"連接錯誤: {str(e)}"
    
    def send(self, message: NotificationMessage) -> Tuple[bool, str]:
        if not self.is_enabled:
            return False, "Email 未啟用"
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"📈 {message.title} - {message.ticker}"
            msg['From'] = self.sender
            msg['To'] = self.recipients
            
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h3 style="color: #667eea;">{message.title}</h3>
                <hr>
                <p><strong>股票:</strong> {message.ticker}</p>
                <p><strong>現價:</strong> ${message.current_price:.3f}</p>
                <p><strong>類型:</strong> {message.alert_type}</p>
                <p><strong>內容:</strong> {message.body}</p>
                <hr>
                <p style="color: #888; font-size: 12px;">🕐 {message.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html', 'utf-8'))
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30)
            
            if self.use_tls:
                server.starttls()
            
            server.login(self.sender, self.password)
            
            recipients_list = [r.strip() for r in self.recipients.split(',')]
            server.sendmail(self.sender, recipients_list, msg.as_string())
            server.quit()
            
            self.stats['sent'] += 1
            self.stats['last_sent'] = datetime.now()
            return True, f"郵件已發送至 {len(recipients_list)} 個收件人"
                
        except Exception as e:
            self.stats['failed'] += 1
            log_error(f"Email 發送錯誤: {e}")
            return False, str(e)


class WebhookProvider(NotificationProvider):
    def __init__(self, config: Dict = None):
        super().__init__("Webhook", config)
        self.webhook_url = self.config.get('url', '')
        self.method = self.config.get('method', 'POST')
        self.is_enabled = bool(self.webhook_url)
    
    def test_connection(self) -> Tuple[bool, str]:
        if not self.webhook_url:
            return False, "缺少 Webhook URL"
        
        try:
            response = requests.get(self.webhook_url, timeout=10)
            
            if response.status_code in [200, 201, 204, 405]:
                return True, f"Webhook 連接成功！HTTP {response.status_code}"
            else:
                return False, f"連接失敗: HTTP {response.status_code}"
                
        except requests.exceptions.ConnectionError:
            return False, "無法連接到 Webhook URL"
        except Exception as e:
            return False, f"連接錯誤: {str(e)}"
    
    def send(self, message: NotificationMessage) -> Tuple[bool, str]:
        if not self.is_enabled:
            return False, "Webhook 未啟用"
        
        try:
            payload = {
                'title': message.title,
                'body': message.body,
                'ticker': message.ticker,
                'alert_type': message.alert_type,
                'alert_value': message.alert_value,
                'current_price': message.current_price,
                'timestamp': message.timestamp.isoformat()
            }
            
            headers = {'Content-Type': 'application/json'}
            
            if self.method.upper() == 'POST':
                response = requests.post(self.webhook_url, json=payload, headers=headers, timeout=30)
            else:
                response = requests.put(self.webhook_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code in [200, 201, 202, 204]:
                self.stats['sent'] += 1
                self.stats['last_sent'] = datetime.now()
                return True, f"Webhook 請求成功 (HTTP {response.status_code})"
            else:
                self.stats['failed'] += 1
                return False, f"請求失敗: HTTP {response.status_code}"
                
        except Exception as e:
            self.stats['failed'] += 1
            log_error(f"Webhook 發送錯誤: {e}")
            return False, str(e)


class NotificationManager:
    def __init__(self):
        self.providers: Dict[str, NotificationProvider] = {}
        self.db = None
        
        log_info("通知管理器初始化")
    
    def initialize(self, db: DatabaseManager):
        self.db = db
        
        if db:
            configs = db.get_all_notification_configs()
            for config_data in configs:
                self.add_provider(config_data['platform'], config_data['config'], config_data['is_enabled'])
        
        log_info(f"通知管理器初始化完成，已加載 {len(self.providers)} 個通知渠道")
    
    def add_provider(self, platform: str, config: Dict, is_enabled: bool = False):
        provider_map = {
            'telegram': TelegramProvider,
            'line': LineProvider,
            'whatsapp': WhatsAppProvider,
            'email': EmailProvider,
            'webhook': WebhookProvider
        }
        
        provider_class = provider_map.get(platform.lower())
        if provider_class:
            provider = provider_class(config)
            provider.is_enabled = is_enabled
            self.providers[platform.lower()] = provider
            log_info(f"已添加 {platform} 通知渠道 (啟用: {is_enabled})")
    
    def send_notification(self, message: NotificationMessage, platforms: List[str] = None) -> Dict[str, Tuple[bool, str]]:
        results = {}
        target_platforms = platforms or list(self.providers.keys())
        
        for platform in target_platforms:
            provider = self.get_provider(platform)
            if provider and provider.is_enabled:
                success, msg = provider.send(message)
                results[platform] = (success, msg)
            else:
                results[platform] = (False, f"{platform} 未配置或未啟用")
        
        return results
    
    def send_alert(self, ticker: str, alert_type: str, alert_value: float, 
                   current_price: float, indicators: Dict = None, send_notification: bool = True) -> Dict[str, Tuple[bool, str]]:
        if not send_notification:
            log_info(f"[靜默模式] {ticker} 觸發 {alert_type} 條件 (不發送通知)")
            return {}
        
        title_map = {
            'price_above': f'{ticker} 價格突破 {alert_value}',
            'price_below': f'{ticker} 價格跌破 {alert_value}',
            'rsi_above': f'{ticker} RSI 超買 ({alert_value})',
            'rsi_below': f'{ticker} RSI 超賣 ({alert_value})',
            'volume_above': f'{ticker} 量能異常放大',
            'ma20_breakout': f'{ticker} 站上 MA20',
            'ma100_breakout': f'{ticker} 站上 MA100',
            'background_update': f'{ticker} 背景更新完成'
        }
        
        message = NotificationMessage(
            title=title_map.get(alert_type, f'{ticker} 警報'),
            body=f'{ticker} 觸發 {alert_type} 條件',
            ticker=ticker,
            alert_type=alert_type,
            alert_value=alert_value,
            current_price=current_price,
            indicators=indicators or {}
        )
        
        return self.send_notification(message)
    
    def get_provider(self, platform: str) -> Optional[NotificationProvider]:
        return self.providers.get(platform.lower())
    
    def get_all_providers(self) -> List[Dict]:
        return [p.get_stats() for p in self.providers.values()]


# ============================================================================
# 14. 背景自動更新系統 (指定時間更新 + 進度顯示)
# ============================================================================
class BackgroundTaskManager:
    def __init__(self):
        self.db = None
        self.stocks_df = None
        self.yf_downloader = None
        self.notification_manager = None
        
        self.is_running = False
        self.scheduler_thread = None
        self.cancel_requested = False
        self._task_lock = threading.Lock()
        self._db_lock = threading.Lock()
        
        self.task_status = {
            'auto_update': {
                'running': False,
                'last_run': None,
                'total_runs': 0,
                'total_updates': 0,
                'total_failures': 0,
                'status': 'stopped'
            }
        }
        
        log_info("背景任務管理器初始化")
    
    def initialize(self, db: DatabaseManager, stocks_df: pd.DataFrame, 
                   yf_downloader: YahooFinanceDownloader, notification_manager: NotificationManager = None):
        self.db = db
        self.stocks_df = stocks_df
        self.yf_downloader = yf_downloader
        self.notification_manager = notification_manager
        
        if self.db:
            try:
                existing_task = self.db.get_background_task('auto_update')
                if existing_task and existing_task.get('result'):
                    result = existing_task.get('result', {})
                    self.task_status['auto_update']['total_runs'] = result.get('total_runs', 0)
                    self.task_status['auto_update']['total_updates'] = result.get('updated', 0)
                    self.task_status['auto_update']['total_failures'] = result.get('failed', 0)
            except Exception as e:
                log_error(f"恢復任務狀態失敗: {e}")
        
        log_info(f"背景任務管理器初始化完成，股票數: {len(stocks_df)}")
        log_info("背景自動更新模式: 靜默執行 + 指定時間更新")
    
    def _get_next_run_time(self, update_time: str = "12:00") -> datetime:
        try:
            now = datetime.now()
            hour, minute = map(int, update_time.split(':'))
            
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            if now >= next_run:
                next_run = next_run + timedelta(days=1)
            
            return next_run
        except:
            return datetime.now().replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(days=1)
    
    def start(self, update_time: str = "12:00", max_stocks: int = 676, outdated_days: int = 1) -> bool:
        if self.is_running:
            log_warning("背景任務已在運行")
            return False
        
        self.is_running = True
        self.cancel_requested = False
        
        next_run = self._get_next_run_time(update_time)
        
        if self.db:
            try:
                self.db.save_background_task(
                    task_name='auto_update',
                    task_type='scheduled_stock_update',
                    config={
                        'update_time': update_time,
                        'max_stocks': max_stocks,
                        'outdated_days': outdated_days,
                        'notify': False,
                        'mode': 'scheduled'
                    },
                    next_run=next_run
                )
                self.db.update_background_task(task_id=-1, status='scheduled')
            except Exception as e:
                log_error(f"保存任務配置失敗: {e}")
        
        self.task_status['auto_update']['status'] = 'scheduled'
        
        self.scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            args=(update_time, max_stocks, outdated_days),
            daemon=True
        )
        self.scheduler_thread.start()
        
        log_info(f"背景自動更新已啟動，指定時間: {update_time}, 下次執行: {next_run}")
        return True
    
    def stop(self) -> bool:
        if not self.is_running:
            return False
        
        self.is_running = False
        self.cancel_requested = True
        
        if self.db:
            try:
                self.db.update_background_task(
                    task_id=-1,
                    status='stopped',
                    error_message='用戶手動停止'
                )
            except Exception as e:
                log_error(f"更新任務狀態失敗: {e}")
        
        self.task_status['auto_update']['status'] = 'stopped'
        log_info("背景自動更新已停止")
        return True
    
    def cancel_current_task(self) -> bool:
        if self.task_status['auto_update']['running']:
            self.cancel_requested = True
            
            if self.db:
                try:
                    self.db.update_background_task(
                        task_id=-1,
                        status='cancelled',
                        error_message='用戶取消任務'
                    )
                except Exception as e:
                    log_error(f"更新任務狀態失敗: {e}")
            
            self.task_status['auto_update']['running'] = False
            self.task_status['auto_update']['status'] = 'cancelled'
            
            log_info("已取消當前任務")
            return True
        
        return False
    
    def _scheduler_loop(self, update_time: str, max_stocks: int, outdated_days: int):
        while self.is_running:
            try:
                now = datetime.now()
                hour, minute = map(int, update_time.split(':'))
                target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                
                should_run = False
                
                if self.db:
                    try:
                        existing_task = self.db.get_background_task('auto_update')
                        if existing_task and existing_task.get('next_run'):
                            next_run = existing_task['next_run']
                            if isinstance(next_run, str):
                                next_run = datetime.fromisoformat(next_run.replace('Z', '+00:00'))
                            if now >= next_run:
                                should_run = True
                        elif self.task_status['auto_update']['last_run'] is None:
                            if now >= target_time:
                                should_run = True
                    except Exception as e:
                        log_error(f"檢查任務調度失敗: {e}")
                        if now < target_time:
                            sleep_seconds = (target_time - now).total_seconds()
                            if sleep_seconds > 0:
                                time.sleep(min(sleep_seconds, 60))
                            continue
                else:
                    if now >= target_time and self.task_status['auto_update']['last_run'] is None:
                        should_run = True
                
                if should_run and not self.cancel_requested:
                    self._run_update_task(max_stocks, outdated_days)
                    
                    next_run = self._get_next_run_time(update_time)
                    if self.db:
                        try:
                            self.db.update_background_task(
                                task_id=-1,
                                status='scheduled',
                                next_run=next_run
                            )
                        except:
                            pass
                
                for _ in range(30):
                    if not self.is_running or self.cancel_requested:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                log_error(f"背景任務調度錯誤: {e}")
                time.sleep(60)
    
    def _run_update_task(self, max_stocks: int, outdated_days: int):
        global BACKGROUND_TASK_PROGRESS
        
        if not self._task_lock.acquire(blocking=False):
            log_warning("有其他任務正在運行，跳過此次執行")
            return
        
        try:
            if self.task_status['auto_update']['running'] and not self.cancel_requested:
                log_warning("更新任務已在運行中，跳過")
                return
            
            self.task_status['auto_update']['running'] = True
            self.cancel_requested = False
            self.task_status['auto_update']['status'] = 'running'
            
            with _bg_progress_lock:
                BACKGROUND_TASK_PROGRESS = {
                    'is_running': True,
                    'current_stock': None,
                    'current_stock_name': None,
                    'processed': 0,
                    'total': 0,
                    'updated': 0,
                    'failed': 0,
                    'qualified': 0,
                    'start_time': time.time(),
                    'logs': [],
                    'last_update': time.time()
                }
            
            if self.db is None:
                try:
                    self.db = DatabaseManager()
                    if self.stocks_df is None:
                        self.stocks_df = load_stock_list()
                    if self.yf_downloader is None:
                        global yf_downloader
                        if yf_downloader is None:
                            yf_downloader = YahooFinanceDownloader()
                        self.yf_downloader = yf_downloader
                except Exception as e:
                    log_error(f"初始化背景任務管理器失敗: {e}")
                    return
            
            start_time = time.time()
            updated = 0
            failed = 0
            qualified_count = 0
            
            try:
                log_info("開始執行背景自動更新 (靜默模式，不發送通知)...")
                
                with _bg_progress_lock:
                    BACKGROUND_TASK_PROGRESS['logs'].append({
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'icon': '🚀',
                        'message': '開始執行背景自動更新'
                    })
                
                with self._db_lock:
                    outdated_stocks = self.db.get_outdated_stocks(self.stocks_df, outdated_days)
                
                if not outdated_stocks:
                    with _bg_progress_lock:
                        BACKGROUND_TASK_PROGRESS['logs'].append({
                            'time': datetime.now().strftime('%H:%M:%S'),
                            'icon': '✅',
                            'message': '沒有需要更新的股票'
                        })
                        BACKGROUND_TASK_PROGRESS['is_running'] = False
                    
                    log_info("沒有需要更新的股票")
                    self.task_status['auto_update']['running'] = False
                    self.task_status['auto_update']['status'] = 'idle'
                    return
                
                stocks_to_update = outdated_stocks[:max_stocks]
                
                with _bg_progress_lock:
                    BACKGROUND_TASK_PROGRESS['total'] = len(stocks_to_update)
                    BACKGROUND_TASK_PROGRESS['logs'].append({
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'icon': '📊',
                        'message': f'需要更新 {len(stocks_to_update)} 檔股票'
                    })
                
                log_info(f"需要更新 {len(stocks_to_update)} 檔股票")
                
                for idx, stock in enumerate(stocks_to_update):
                    if self.cancel_requested or not self.is_running:
                        with _bg_progress_lock:
                            BACKGROUND_TASK_PROGRESS['logs'].append({
                                'time': datetime.now().strftime('%H:%M:%S'),
                                'icon': '🛑',
                                'message': '任務已被取消或停止'
                            })
                        break
                    
                    try:
                        ticker = stock['ticker']
                        code = stock['code']
                        name = stock['name']
                        
                        with _bg_progress_lock:
                            BACKGROUND_TASK_PROGRESS['current_stock'] = ticker
                            BACKGROUND_TASK_PROGRESS['current_stock_name'] = name
                            BACKGROUND_TASK_PROGRESS['processed'] = idx
                            BACKGROUND_TASK_PROGRESS['last_update'] = time.time()
                        
                        df = self.yf_downloader.download(ticker, period="1y")
                        
                        if df is None or df.empty or len(df) < 30:
                            failed += 1
                            with _bg_progress_lock:
                                BACKGROUND_TASK_PROGRESS['failed'] = failed
                                BACKGROUND_TASK_PROGRESS['logs'].append({
                                    'time': datetime.now().strftime('%H:%M:%S'),
                                    'icon': '❌',
                                    'message': f'{ticker} {name} - 無法獲取數據'
                                })
                            continue
                        
                        indicators = TechnicalIndicators.calculate_all(df)
                        
                        if indicators is None:
                            failed += 1
                            with _bg_progress_lock:
                                BACKGROUND_TASK_PROGRESS['failed'] = failed
                            continue
                        
                        saved = self.db.save_price_and_indicators(code, df, indicators, 0)
                        
                        if saved:
                            updated += 1
                            with _bg_progress_lock:
                                BACKGROUND_TASK_PROGRESS['updated'] = updated
                            
                            close = indicators['close']
                            ma20 = indicators.get('ma20', close)
                            ma100 = indicators.get('ma100', close)
                            vol_ratio = indicators.get('volume_ratio', 1)
                            ret_5d = indicators.get('ret_5d', 0)
                            ret_3d = indicators.get('ret_3d', 0)
                            price_strength = indicators.get('price_strength', 50)
                            
                            is_strong = (close > ma20 and close > ma100 and 
                                        vol_ratio > 1.5 and (ret_5d > 5 or ret_3d > 8) and
                                        price_strength > 80)
                            
                            if is_strong:
                                qualified_count += 1
                                with _bg_progress_lock:
                                    BACKGROUND_TASK_PROGRESS['qualified'] = qualified_count
                                    BACKGROUND_TASK_PROGRESS['logs'].append({
                                        'time': datetime.now().strftime('%H:%M:%S'),
                                        'icon': '⭐',
                                        'message': f'強勢股: {ticker} {name} (強度:{price_strength:.0f})'
                                    })
                                    if len(BACKGROUND_TASK_PROGRESS['logs']) > 30:
                                        BACKGROUND_TASK_PROGRESS['logs'].pop(0)
                        
                    except Exception as e:
                        failed += 1
                        with _bg_progress_lock:
                            BACKGROUND_TASK_PROGRESS['failed'] = failed
                            BACKGROUND_TASK_PROGRESS['logs'].append({
                                'time': datetime.now().strftime('%H:%M:%S'),
                                'icon': '❌',
                                'message': f'{ticker} {name} - {str(e)[:50]}'
                            })
                        log_error(f"更新股票 {stock['ticker']} 失敗: {e}")
                    
                    time.sleep(1)
                
                elapsed = time.time() - start_time
                
                self.task_status['auto_update']['last_run'] = datetime.now()
                self.task_status['auto_update']['total_runs'] += 1
                self.task_status['auto_update']['total_updates'] += updated
                self.task_status['auto_update']['total_failures'] += failed
                
                with _bg_progress_lock:
                    BACKGROUND_TASK_PROGRESS['logs'].append({
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'icon': '🎉',
                        'message': f'完成! 更新:{updated}, 失敗:{failed}, 強勢:{qualified_count}, 用時:{elapsed:.1f}秒'
                    })
                    BACKGROUND_TASK_PROGRESS['is_running'] = False
                    BACKGROUND_TASK_PROGRESS['processed'] = len(stocks_to_update)
                
                if self.db and self.is_running and not self.cancel_requested:
                    try:
                        self.db.update_background_task(
                            task_id=-1,
                            status='completed',
                            result={
                                'updated': updated,
                                'failed': failed,
                                'qualified': qualified_count,
                                'elapsed': elapsed,
                                'total_runs': self.task_status['auto_update']['total_runs']
                            }
                        )
                    except Exception as e:
                        log_error(f"保存任務結果失敗: {e}")
                
                self.task_status['auto_update']['running'] = False
                if self.is_running:
                    self.task_status['auto_update']['status'] = 'scheduled'
                else:
                    self.task_status['auto_update']['status'] = 'stopped'
                
                log_info(f"背景更新完成 (靜默): 更新 {updated} 檔，失敗 {failed} 檔，符合 {qualified_count} 檔，用時 {elapsed:.1f}秒")
                
            except Exception as e:
                log_error(f"背景更新任務錯誤: {e}")
                self.task_status['auto_update']['running'] = False
                self.task_status['auto_update']['status'] = 'error'
                
                with _bg_progress_lock:
                    BACKGROUND_TASK_PROGRESS['is_running'] = False
                    BACKGROUND_TASK_PROGRESS['logs'].append({
                        'time': datetime.now().strftime('%H:%M:%S'),
                        'icon': '❌',
                        'message': f'任務錯誤: {str(e)[:100]}'
                    })
                
                if self.db:
                    try:
                        self.db.update_background_task(
                            task_id=-1,
                            status='error',
                            error_message=str(e)
                        )
                    except:
                        pass
        
        finally:
            self._task_lock.release()
    
    def get_status(self) -> Dict:
        return {
            'is_running': self.is_running,
            'auto_update': self.task_status['auto_update']
        }
    
    def get_progress(self) -> Dict:
        global BACKGROUND_TASK_PROGRESS
        with _bg_progress_lock:
            progress = BACKGROUND_TASK_PROGRESS.copy()
            
            if progress['total'] > 0:
                progress['percentage'] = (progress['processed'] / progress['total']) * 100
            else:
                progress['percentage'] = 0
            
            if progress['processed'] > 0 and progress['start_time']:
                elapsed = time.time() - progress['start_time']
                avg_time_per_stock = elapsed / progress['processed']
                remaining = progress['total'] - progress['processed']
                progress['eta_seconds'] = remaining * avg_time_per_stock
            else:
                progress['eta_seconds'] = 0
            
            if progress['start_time']:
                total_elapsed = time.time() - progress['start_time']
                if total_elapsed > 0:
                    progress['speed'] = progress['processed'] / total_elapsed
                else:
                    progress['speed'] = 0
            else:
                progress['speed'] = 0
            
            return progress
    
    def run_now(self, max_stocks: int = 676, outdated_days: int = 1, notify: bool = False) -> Tuple[bool, str]:
        if self.task_status['auto_update']['running']:
            return False, "任務已在運行中"
        
        self.current_task_thread = threading.Thread(
            target=self._run_update_task,
            args=(max_stocks, outdated_days),
            daemon=True
        )
        self.current_task_thread.start()
        
        return True, "任務已啟動"


# ============================================================================
# 15. 核心分析函數
# ============================================================================
def analyze_stock_task(args: Tuple) -> Tuple[Optional[Dict], bool, bool]:
    ticker, code, name, industry, batch_id, force_update, data_period, \
    enable_ma20_breakout, enable_ma100_breakout, enable_volume_surge, \
    enable_momentum, enable_trend_following, enable_rsi, enable_macd, \
    min_ret_5d, min_ret_3d, min_volume_ratio, min_price_strength, max_rsi = args
    
    global yf_downloader, notification_manager
    
    try:
        if yf_downloader is None:
            return None, False, False
        
        db = DatabaseManager()
        
        df = yf_downloader.download(ticker, period=data_period)
        
        if df is None or df.empty or len(df) < 30:
            return None, False, False
        
        indicators = TechnicalIndicators.calculate_all(df)
        
        if indicators is None:
            return None, True, False
        
        saved = db.save_price_and_indicators(code, df, indicators, batch_id)
        
        if not saved:
            return None, True, False
        
        close = indicators['close']
        ma20 = indicators.get('ma20', close)
        ma100 = indicators.get('ma100', close)
        vol_ratio = indicators.get('volume_ratio', 1)
        ret_5d = indicators.get('ret_5d', 0)
        ret_3d = indicators.get('ret_3d', 0)
        price_strength = indicators.get('price_strength', 50)
        rsi = indicators.get('rsi', 50)
        macd_trend = indicators.get('macd_trend', 'neutral')
        
        cond1 = (close > ma20) if enable_ma20_breakout else True
        cond1b = (close > ma100) if enable_ma100_breakout else True
        cond2 = (vol_ratio > min_volume_ratio) if enable_volume_surge else True
        cond3 = (ret_5d > min_ret_5d or ret_3d > min_ret_3d) if enable_momentum else True
        cond4 = (price_strength > min_price_strength) if enable_trend_following else True
        cond5 = (rsi < max_rsi) if enable_rsi else True
        cond6 = (macd_trend == 'bullish') if enable_macd else True
        
        qualifies = cond1 and cond1b and cond2 and cond3 and cond4 and cond5 and cond6
        
        if qualifies:
            result = {
                '代碼': code, '名稱': name, '行業': industry, '最新價': round(close, 3),
                'MA5': round(indicators.get('ma5', close), 3),
                'MA10': round(indicators.get('ma10', close), 3),
                'MA20': round(ma20, 3),
                'MA50': round(indicators.get('ma50', close), 3),
                'MA100': round(ma100, 3),
                'RSI(14)': round(rsi, 1),
                'MACD': macd_trend,
                '量比': round(vol_ratio, 2),
                'Stoch%K': round(indicators.get('stoch_k', 0), 1),
                'Stoch%D': round(indicators.get('stoch_d', 0), 1),
                'ROC(5)': round(indicators.get('roc_5', 0), 2),
                '5日漲幅%': round(ret_5d, 2),
                '3日漲幅%': round(ret_3d, 2),
                '20日漲幅%': round(indicators.get('ret_20d', 0), 2),
                '價格強度': round(price_strength, 1),
                'ATR%': round(indicators.get('atr_percent', 0), 2),
                '站上MA20': '是' if close > ma20 else '否',
                '站上MA100': '是' if close > ma100 else '否',
                'Ticker': ticker
            }
            return result, True, True
        
        return None, True, False
        
    except Exception as e:
        log_error(f"分析股票 {ticker} 失敗: {e}")
        return None, False, False


# ============================================================================
# 16. 批量更新函數
# ============================================================================
def batch_update(stocks_df: pd.DataFrame, force_update: bool = False,
                max_stocks: int = None, data_period: str = "1y", workers: int = 4,
                enable_ma20_breakout: bool = True, enable_ma100_breakout: bool = False,
                enable_volume_surge: bool = True, enable_momentum: bool = True,
                enable_trend_following: bool = True, enable_rsi: bool = False,
                enable_macd: bool = False, min_ret_5d: float = 3.0, min_ret_3d: float = 5.0,
                min_volume_ratio: float = 1.2, min_price_strength: float = 60.0,
                max_rsi: float = 85.0) -> Tuple[List[Dict], int, int, int]:
    global yf_downloader, notification_manager
    
    start_time = time.time()
    results = []
    
    if yf_downloader is None:
        st.error("系統未初始化")
        return results, 0, 0, 0
    
    db = DatabaseManager()
    db.save_stock_info_batch(stocks_df)
    
    if notification_manager is None:
        notification_manager = NotificationManager()
    notification_manager.initialize(db)
    
    total_stocks_available = len(stocks_df)
    if max_stocks is None or max_stocks > total_stocks_available:
        max_stocks = total_stocks_available
    
    mode_text = "完整更新" if force_update else "增量更新"
    
    if not force_update:
        stocks_to_update = [row for _, row in stocks_df.iterrows() if db.needs_update(row['code'])]
        
        if len(stocks_to_update) == 0:
            st.info("所有股票數據都是最新的")
            return results, 0, 0, 0
        
        stocks_df = pd.DataFrame(stocks_to_update)
    
    stocks_df = stocks_df.head(max_stocks)
    total_stocks = len(stocks_df)
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    stats_text = st.empty()
    log_text = st.empty()
    logs = []
    
    control_col1, control_col2 = st.columns(2)
    pause_btn = control_col1.button("⏸️ 暫停", width='stretch')
    stop_btn = control_col2.button("⏹️ 停止", width='stretch')
    
    if pause_btn:
        pause_update()
    if stop_btn:
        stop_update()
    
    def add_log(icon, message):
        timestamp = datetime.now().strftime('%H:%M:%S')
        logs.append(f"| {timestamp} | {icon} | {message} |")
        if len(logs) > 15:
            logs.pop(0)
    
    add_log('🚀', f'開始更新 (模式: {mode_text})')
    add_log('📊', f'共 {total_stocks}/{total_stocks_available} 檔 (最大: {max_stocks})')
    
    batch_id = db.start_batch(total_stocks)
    
    tasks = []
    for row in stocks_df.itertuples():
        industry = getattr(row, 'industry', '其他')
        tasks.append((
            row.ticker, row.code, row.name, industry,
            batch_id, force_update, data_period,
            enable_ma20_breakout, enable_ma100_breakout, enable_volume_surge,
            enable_momentum, enable_trend_following, enable_rsi, enable_macd,
            min_ret_5d, min_ret_3d, min_volume_ratio, min_price_strength, max_rsi
        ))
    
    speed_times = []
    last_update = time.time()
    last_processed = 0
    
    processed = updated = failed = qualifies_count = 0
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_task = {executor.submit(analyze_stock_task, task): task for task in tasks}
        
        for future in as_completed(future_to_task):
            should_continue, status_msg = check_pause_or_stop()
            
            if not should_continue:
                for f in future_to_task:
                    f.cancel()
                
                if _UPDATE_STATE['is_stopped']:
                    add_log('🛑', '用戶停止更新')
                    db.end_batch(batch_id, updated, failed, total_stocks - processed, 'stopped')
                else:
                    add_log('⏸️', '更新已暫停')
                    db.end_batch(batch_id, updated, failed, total_stocks - processed, 'paused')
                
                break
            
            task = future_to_task[future]
            ticker = task[0]
            name = task[2]
            
            try:
                result, data_updated, qualifies = future.result(timeout=120)
                
                if data_updated:
                    updated += 1
                
                if qualifies:
                    qualifies_count += 1
                    results.append(result)
                    add_log('✅', f'{ticker} {name}')
                else:
                    if result is None:
                        failed += 1
                        add_log('❌', f'{ticker} {name}')
                    else:
                        add_log('📌', f'{ticker} {name}')
                        
            except Exception as e:
                failed += 1
                add_log('❌', f'{ticker} {name}')
            
            processed += 1
            
            current_time = time.time()
            elapsed = current_time - start_time
            
            if processed > last_processed:
                speed = (processed - last_processed) / (current_time - last_update)
                speed_times.append(speed)
                if len(speed_times) > 10:
                    speed_times.pop(0)
                last_update = current_time
                last_processed = processed
            
            avg_speed = np.mean(speed_times) if speed_times else 1.0
            remaining = total_stocks - processed
            eta = remaining / avg_speed if avg_speed > 0 else 0
            
            progress = processed / total_stocks
            progress_bar.progress(progress)
            
            status_text.markdown(f"### 📊 更新進度 | **{progress*100:.1f}%**")
            
            stats_text.markdown(f"""
            | 已處理 | 已更新 | 失敗 | 符合 |
            |:------:|:------:|:----:|:----:|
            | {processed} | {updated} | {failed} | {qualifies_count} |
            
            ⚡ {avg_speed:.1f} 檔/秒 | ⏱️ {eta:.0f}秒 | 🕐 {elapsed:.1f}秒
            """)
            
            log_text.markdown(f"#### 實時日誌\n\n" + "\n".join(logs))
    
    elapsed = time.time() - start_time
    
    download_stats = yf_downloader.get_stats()
    add_log('📈', f'下載: 成功={download_stats["successful_requests"]}, 失敗={download_stats["failed_requests"]}')
    
    if notification_manager:
        notif_stats = notification_manager.get_all_providers()
        total_sent = sum(p.get('sent', 0) for p in notif_stats)
        total_failed = sum(p.get('failed', 0) for p in notif_stats)
        add_log('📲', f'通知: 成功={total_sent}, 失敗={total_failed}')
    
    if not _UPDATE_STATE['is_stopped'] and not _UPDATE_STATE['is_paused']:
        db.end_batch(batch_id, updated, failed, qualifies_count, 'completed')
        add_log('🎉', f'完成！用時 {elapsed:.1f}秒，找到 {qualifies_count} 檔符合條件')
    
    progress_bar.empty()
    
    if _UPDATE_STATE['is_paused']:
        if st.button("▶️ 繼續更新", type="primary", width='stretch'):
            resume_update()
            st.rerun()
    
    status_text.markdown("### ✅ 更新完成！")
    log_text.markdown(f"#### 最終日誌\n\n" + "\n".join(logs))
    
    if not _UPDATE_STATE['is_stopped']:
        st.success(f"""
        ✅ 更新完成！
        
        - **處理:** {processed} 檔 / 共 {total_stocks_available} 檔
        - **成功:** {updated} 檔
        - **失敗:** {failed} 檔
        - **符合條件:** {qualifies_count} 檔
        - **用時:** {elapsed:.1f}秒
        
        下載成功率: {download_stats['success_rate']}
        """)
    
    return results, updated, failed, qualifies_count


# ============================================================================
# 17. 統一篩選函數
# ============================================================================
def filter_strong_stocks(indicators_df: pd.DataFrame,
                         enable_ma20_breakout: bool = True, enable_ma100_breakout: bool = False,
                         enable_volume_surge: bool = True, enable_momentum: bool = True,
                         enable_trend_following: bool = True, enable_rsi: bool = False,
                         enable_macd: bool = False, min_ret_5d: float = 3.0, min_ret_3d: float = 5.0,
                         min_volume_ratio: float = 1.2, min_price_strength: float = 60.0,
                         max_rsi: float = 85.0) -> pd.DataFrame:
    if indicators_df.empty:
        return indicators_df
    
    filtered = indicators_df.copy()
    
    for col in ['close', 'ma20', 'ma100', 'volume_ratio', 'ret_5d', 'ret_3d', 'price_strength', 'rsi', 'macd_trend']:
        if col not in filtered.columns:
            if col == 'macd_trend':
                filtered[col] = 'neutral'
            else:
                filtered[col] = 0.0
    
    filtered['close'] = filtered['close'].fillna(0)
    filtered['ma20'] = filtered['ma20'].fillna(filtered['close'])
    filtered['ma100'] = filtered['ma100'].fillna(filtered['close'])
    filtered['volume_ratio'] = filtered['volume_ratio'].fillna(1.0)
    filtered['ret_5d'] = filtered['ret_5d'].fillna(0)
    filtered['ret_3d'] = filtered['ret_3d'].fillna(0)
    filtered['price_strength'] = filtered['price_strength'].fillna(50)
    filtered['rsi'] = filtered['rsi'].fillna(50)
    filtered['macd_trend'] = filtered['macd_trend'].fillna('neutral')
    
    mask1 = filtered['close'] > filtered['ma20'] if enable_ma20_breakout else True
    mask1b = filtered['close'] > filtered['ma100'] if enable_ma100_breakout else True
    mask2 = filtered['volume_ratio'] > min_volume_ratio if enable_volume_surge else True
    mask3 = (filtered['ret_5d'] > min_ret_5d) | (filtered['ret_3d'] > min_ret_3d) if enable_momentum else True
    mask4 = filtered['price_strength'] > min_price_strength if enable_trend_following else True
    mask5 = filtered['rsi'] < max_rsi if enable_rsi else True
    mask6 = filtered['macd_trend'] == 'bullish' if enable_macd else True
    
    return filtered[mask1 & mask1b & mask2 & mask3 & mask4 & mask5 & mask6]


def format_filtered_results(filtered_df: pd.DataFrame) -> pd.DataFrame:
    if filtered_df.empty:
        return filtered_df
    
    filtered_df = filtered_df.copy()
    
    def get_bb_status(row):
        close = row.get('close', 0)
        bb_upper = row.get('bb_upper', close)
        bb_lower = row.get('bb_lower', close)
        if close > bb_upper:
            return '突破'
        elif close < bb_lower:
            return '回調'
        return '盤整'
    
    if 'bb_upper' in filtered_df.columns:
        filtered_df['布林帶'] = filtered_df.apply(get_bb_status, axis=1)
    else:
        filtered_df['布林帶'] = '盤整'
    
    filtered_df['站上MA20'] = filtered_df['close'] > filtered_df['ma20']
    filtered_df['站上MA100'] = filtered_df['close'] > filtered_df['ma100']
    
    display_df = pd.DataFrame()
    display_df['代碼'] = filtered_df['code']
    display_df['名稱'] = filtered_df['name']
    display_df['行業'] = filtered_df.get('industry', '其他')
    display_df['最新價'] = filtered_df['close'].round(3)
    display_df['MA5'] = filtered_df.get('ma5', filtered_df['close']).round(3)
    display_df['MA10'] = filtered_df.get('ma10', filtered_df['close']).round(3)
    display_df['MA20'] = filtered_df['ma20'].round(3)
    display_df['MA50'] = filtered_df.get('ma50', filtered_df['close']).round(3)
    display_df['MA100'] = filtered_df['ma100'].round(3)
    display_df['MA200'] = filtered_df.get('ma200', filtered_df['close']).round(3)
    display_df['RSI(14)'] = filtered_df.get('rsi', 50).round(1)
    display_df['MACD'] = filtered_df.get('macd_trend', 'neutral')
    display_df['布林帶'] = filtered_df.get('布林帶', '盤整')
    display_df['量比'] = filtered_df['volume_ratio'].round(2)
    display_df['Stoch%K'] = filtered_df.get('stoch_k', 50).round(1)
    display_df['Stoch%D'] = filtered_df.get('stoch_d', 50).round(1)
    display_df['ROC(5)'] = filtered_df.get('roc_5', 0).round(2)
    display_df['ATR%'] = filtered_df.get('atr_percent', 0).round(2)
    display_df['5日漲幅%'] = filtered_df['ret_5d'].round(2)
    display_df['3日漲幅%'] = filtered_df['ret_3d'].round(2)
    display_df['20日漲幅%'] = filtered_df.get('ret_20d', 0).round(2)
    display_df['價格強度'] = filtered_df['price_strength'].round(1)
    display_df['站上MA20'] = filtered_df['站上MA20'].apply(lambda x: '是' if x else '否')
    display_df['站上MA100'] = filtered_df['站上MA100'].apply(lambda x: '是' if x else '否')
    
    return display_df


# ============================================================================
# 18. 改進計劃
# ============================================================================
IMPROVEMENT_PLAN = {
    'short_term': {
        'title': '✅ 短期改進 (1-2週)',
        'icon': '✅',
        'items': [
            {'status': 'completed', 'task': '✅ 版本號修正', 'desc': '統一版本號管理'},
            {'status': 'completed', 'task': '✅ 輸入驗證', 'desc': '所有參數增加驗證'},
            {'status': 'completed', 'task': '✅ 錯誤處理', 'desc': '完善錯誤碼和日誌'},
            {'status': 'completed', 'task': '✅ SQLite 並發連接', 'desc': '連接池 + WAL模式 + 重試機制'},
            {'status': 'completed', 'task': '✅ 欄位數量修復', 'desc': '修正 INSERT 語句欄位數量'},
            {'status': 'completed', 'task': '✅ 背景更新進度', 'desc': '實時進度條 + ETA + 日誌'},
        ]
    },
    'mid_term': {
        'title': '✅ 中期改進 (1-2個月)',
        'icon': '✅',
        'items': [
            {'status': 'completed', 'task': '✅ 技術指標', 'desc': 'MA5/10/20/50/100/200, RSI, MACD, 布林帶, Stochastic, ATR'},
            {'status': 'completed', 'task': '✅ 快取機制', 'desc': '減少重複下載'},
            {'status': 'completed', 'task': '✅ 行業分類', 'desc': '完善HSICS分類'},
            {'status': 'completed', 'task': '✅ 批量操作', 'desc': '暫停/停止/恢復'},
            {'status': 'completed', 'task': '✅ 回測系統', 'desc': 'MA交叉策略、RSI策略'},
            {'status': 'completed', 'task': '✅ 警報系統', 'desc': '價格/RSI/量能警報'},
        ]
    },
    'long_term': {
        'title': '✅ 長期改進 (3-6個月) - 已完成',
        'icon': '✅',
        'items': [
            {'status': 'completed', 'task': '✅ 實時推送', 'desc': 'Line/Telegram/WhatsApp/Email/Webhook 即時通知'},
            {'status': 'completed', 'task': '✅ 背景自動更新', 'desc': '自動更新過期數據 + 取消任務 + 任務歷史 + 靜默執行'},
            {'status': 'completed', 'task': '✅ 取消現有警報', 'desc': '一鍵刪除所有已設定的警報'},
            {'status': 'completed', 'task': '✅ SQLite 並發優化', 'desc': '連接池 + WAL + 重試 + 指數退避'},
            {'status': 'completed', 'task': '✅ 背景更新進度', 'desc': '進度條 + ETA + 當前股票 + 即時日誌'},
            {'status': 'completed', 'task': '✅ 指定時間更新', 'desc': '每天固定時間12:00更新，預設不啟用'},
        ]
    }
}


# ============================================================================
# 19. 全局元件初始化
# ============================================================================
def initialize_globals():
    global config, cache_manager, yf_downloader, notification_manager, background_task_manager, stocks_df_global
    
    if config is None:
        config = ConfigManager()
    
    if cache_manager is None:
        cache_manager = CacheManager(
            max_size=config.get('max_cache_size', 2000),
            default_ttl_hours=config.get('cache_ttl_hours', 24),
            max_memory_mb=config.get('max_cache_memory_mb', 200.0)
        )
    
    if yf_downloader is None:
        try:
            yf_downloader = YahooFinanceDownloader(
                rate_limit_per_min=config.get('rate_limit_per_min', 60),
                max_retries=config.get('max_retries', 3),
                timeout_seconds=config.get('timeout_seconds', 30)
            )
            log_info("同步下載器初始化完成")
        except Exception as e:
            log_error(f"同步下載器初始化失敗: {e}")
            raise
    
    if notification_manager is None:
        notification_manager = NotificationManager()
        db = DatabaseManager()
        notification_manager.initialize(db)
        log_info("通知管理器初始化完成")


# ============================================================================
# 20. 回測系統
# ============================================================================
class BacktestSystem:
    def __init__(self, db: DatabaseManager, downloader: YahooFinanceDownloader):
        self.db = db
        self.downloader = downloader
        global config
        if config is None:
            config = ConfigManager()
        self.config = config
    
    def run_backtest(self, ticker: str, strategy: str = "MA_Crossover",
                     start_date: str = None, end_date: str = None,
                     initial_capital: float = 100000.0, position_size: float = 0.1,
                     stop_loss: float = 0.05, take_profit: float = 0.15) -> Dict:
        try:
            df = self.downloader.download(ticker, period="2y")
            
            if df is None or df.empty or len(df) < 50:
                raise AppError(ErrorCode.BACKTEST_NO_DATA, details=f"無法獲取 {ticker} 的歷史數據")
            
            if start_date:
                try:
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    df = df[df.index >= start_dt]
                except:
                    pass
            
            if end_date:
                try:
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    df = df[df.index <= end_dt]
                except:
                    pass
            
            if len(df) < 50:
                raise AppError(ErrorCode.BACKTEST_NO_DATA, details=f"數據不足: {len(df)} 天")
            
            close = df['Close']
            
            if strategy == "MA_Crossover":
                ma_fast = close.rolling(10).mean()
                ma_slow = close.rolling(30).mean()
                signal = (ma_fast > ma_slow).astype(int)
            elif strategy == "RSI_Overbought_Oversold":
                rsi = TechnicalIndicators._calculate_rsi(close, 14)
                signal = (rsi < 30).astype(int)
            else:
                ma20 = close.rolling(20).mean()
                signal = (close > ma20).astype(int)
            
            trades = []
            capital = initial_capital
            position = 0
            entry_price = 0
            
            for i, (date, price) in enumerate(close.items()):
                if signal.iloc[i] == 1 and position == 0:
                    position = (capital * position_size) / price
                    entry_price = price
                elif signal.iloc[i] == 0 and position > 0:
                    pnl_pct = (price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                    pnl_amount = position * price - (position * entry_price)
                    capital = position * price
                    trades.append({
                        'type': 'SELL',
                        'date': str(date.date()) if hasattr(date, 'date') else str(date)[:10],
                        'price': float(price),
                        'pnl_pct': pnl_pct,
                        'pnl_amount': float(pnl_amount),
                        'capital': float(capital)
                    })
                    position = 0
                    entry_price = 0
            
            if position > 0:
                final_price = close.iloc[-1]
                pnl_pct = (final_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                pnl_amount = position * final_price - (position * entry_price)
                capital = position * final_price
                trades.append({
                    'type': 'SELL',
                    'date': str(close.index[-1].date()) if hasattr(close.index[-1], 'date') else str(close.index[-1])[:10],
                    'price': float(final_price),
                    'pnl_pct': pnl_pct,
                    'pnl_amount': float(pnl_amount),
                    'capital': float(capital)
                })
                position = 0
            
            final_capital = capital
            total_return = (final_capital - initial_capital) / initial_capital * 100 if initial_capital > 0 else 0
            
            equity_curve = [initial_capital]
            for trade in trades:
                if 'capital' in trade:
                    equity_curve.append(trade['capital'])
            
            max_drawdown = 0
            peak = equity_curve[0] if equity_curve else initial_capital
            for equity in equity_curve:
                if equity > peak:
                    peak = equity
                drawdown = (peak - equity) / peak * 100 if peak > 0 else 0
                max_drawdown = max(max_drawdown, drawdown)
            
            returns = []
            for i in range(1, len(equity_curve)):
                ret = (equity_curve[i] - equity_curve[i-1]) / equity_curve[i-1] if equity_curve[i-1] > 0 else 0
                returns.append(ret)
            
            sharpe_ratio = 0
            if returns and np.std(returns) > 0:
                sharpe_ratio = (np.mean(returns) / np.std(returns)) * np.sqrt(252) * 100
            
            sell_trades = [t for t in trades if t['type'] == 'SELL']
            winning_trades = [t for t in sell_trades if t.get('pnl_pct', 0) > 0]
            win_rate = len(winning_trades) / len(sell_trades) * 100 if sell_trades else 0
            
            years = len(df) / 252 if len(df) >= 252 else 1
            annual_return = total_return / years if years > 0 else total_return
            
            result = {
                'strategy_name': strategy,
                'ticker': ticker,
                'start_date': str(df.index[0].date()) if hasattr(df.index[0], 'date') else str(df.index[0])[:10],
                'end_date': str(df.index[-1].date()) if hasattr(df.index[-1], 'date') else str(df.index[-1])[:10],
                'initial_capital': initial_capital,
                'final_capital': final_capital,
                'total_return': round(total_return, 2),
                'annual_return': round(annual_return, 2),
                'max_drawdown': round(max_drawdown, 2),
                'sharpe_ratio': round(sharpe_ratio, 2),
                'win_rate': round(win_rate, 2),
                'num_trades': len(sell_trades)
            }
            
            self.db.save_backtest_result(result)
            
            return result
            
        except AppError:
            raise
        except Exception as e:
            log_error(f"回測失敗: {e}")
            raise AppError(ErrorCode.BACKTEST_ERROR, details=str(e))


# ============================================================================
# 21. Streamlit UI (完整修復版)
# ============================================================================
def main():
    initialize_globals()
    
    global notification_manager, background_task_manager, stocks_df_global, BACKGROUND_TASK_PROGRESS
    
    st.set_page_config(
        page_title=f"港股強勢股篩選器 V{APP_VERSION}",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title(f"📈 港股強勢股篩選器 V{APP_VERSION}")
    st.markdown("""
    <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); padding: 15px; border-radius: 10px; margin-bottom: 20px;">
        <div style="color: white; font-size: 14px;">
            <strong>【核心邏輯】</strong><br>
            📌 股價站上MA20 & MA100 &nbsp;|&nbsp; 📊 量能放大 1.5x &nbsp;|&nbsp; 📈 短線漲幅 5/8% &nbsp;|&nbsp; 💹 價格強度 80+<br>
            <hr style="border-color: rgba(255,255,255,0.3); margin: 8px 0;">
            <strong>【SQLite 並發優化】</strong><br>
            <span style="font-size: 12px;">✅ 連接池管理 (10個連接) | ✅ WAL模式 | ✅ Busy Timeout (60秒) | ✅ 指數退避重試 (最多5次) | ✅ 任務鎖保護</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.sidebar.header("⚙️ 系統配置")
    stocks_df = load_stock_list()
    stocks_df_global = stocks_df
    
    if stocks_df is None:
        st.error("無法載入股票清單")
        st.stop()
    
    total_stocks = len(stocks_df)
    st.sidebar.markdown(f"**股票總數:** {total_stocks} 檔")
    
    industry_counts = stocks_df['industry'].value_counts()
    st.sidebar.markdown("### 行業分布")
    for industry, count in industry_counts.head(10).items():
        st.sidebar.markdown(f"- {industry}: {count} 檔")
    
    st.sidebar.markdown("---")
    
    st.sidebar.header("📥 更新設置")
    col_update1, col_update2 = st.sidebar.columns(2)
    with col_update1:
        update_mode = st.radio("更新模式", ["增量更新", "完整更新"], index=0)
    with col_update2:
        force_update = (update_mode == "完整更新")
    
    max_stocks = st.sidebar.slider("最大更新數量", 10, min(676, total_stocks), min(676, total_stocks), 10)
    st.sidebar.caption(f"⚠️ 最大可選: {total_stocks} 檔 (股票總數)")
    
    data_period = st.sidebar.select_slider("數據週期", options=["3mo", "6mo", "1y", "2y"], value="1y")
    
    st.sidebar.markdown("---")
    
    st.sidebar.header("🎯 核心邏輯")
    enable_ma20 = st.sidebar.checkbox("1️⃣ 股價站上20日均線", value=True)
    enable_ma100 = st.sidebar.checkbox("2️⃣ 股價站上100日均線", value=True)
    enable_volume = st.sidebar.checkbox("3️⃣ 放量起漲", value=True)
    enable_momentum = st.sidebar.checkbox("4️⃣ 強勢噴出", value=True)
    enable_trend = st.sidebar.checkbox("5️⃣ 順勢交易", value=True)
    
    st.sidebar.markdown("---")
    
    st.sidebar.header("📊 擴展技術指標")
    enable_rsi = st.sidebar.checkbox("RSI 條件 (避免超買)", value=False)
    if enable_rsi:
        max_rsi = st.sidebar.slider("RSI 最大值", 50, 95, 80, 5)
    else:
        max_rsi = 100
    
    enable_macd = st.sidebar.checkbox("MACD 多頭確認", value=False)
    
    st.sidebar.markdown("---")
    
    st.sidebar.header("🎚️ 篩選參數")
    
    min_5d_return = st.sidebar.slider("最小5日漲幅 (%)", 0.0, 20.0, 5.0, 0.5)
    min_3d_return = st.sidebar.slider("最小3日漲幅 (%)", 0.0, 20.0, 8.0, 0.5)
    volume_ratio = st.sidebar.slider("量能倍數", 0.5, 3.0, 1.5, 0.1)
    min_price_strength = st.sidebar.slider("價格強度閾值", 30, 100, 80, 1)
    
    st.sidebar.markdown("---")
    st.sidebar.header("🔧 管理功能")
    
    col_db1, col_db2 = st.sidebar.columns(2)
    with col_db1:
        if st.button("清空緩存", width='stretch'):
            if cache_manager:
                cache_manager.clear()
                st.sidebar.success("緩存已清空！")
                st.rerun()
    with col_db2:
        if st.button("重置配置", width='stretch'):
            try:
                config_path = Path("config.json")
                if config_path.exists():
                    config_path.unlink()
                st.sidebar.success("配置已重置！")
            except Exception as e:
                st.sidebar.error(f"重置失敗: {e}")
    
    if cache_manager:
        st.sidebar.markdown("---")
        st.sidebar.header("📈 緩存狀態")
        cache_stats = cache_manager.get_stats()
        st.sidebar.markdown(f"- **命中率:** {cache_stats['hit_rate']}\n- **項目數:** {cache_stats['items']}")
    
    if yf_downloader:
        st.sidebar.markdown("---")
        st.sidebar.header("📡 下載狀態")
        dl_stats = yf_downloader.get_stats()
        st.sidebar.markdown(f"- **成功率:** {dl_stats['success_rate']}\n- **成功:** {dl_stats['successful_requests']}\n- **失敗:** {dl_stats['failed_requests']}")
    
    st.sidebar.markdown("---")
    st.sidebar.header("🗄️ SQLite 連接池")
    try:
        db = DatabaseManager()
        pool_stats = db.get_pool_stats()
        st.sidebar.markdown(f"- **池大小:** {pool_stats['pool_size']}")
        st.sidebar.markdown(f"- **使用中:** {pool_stats['in_use']}")
        st.sidebar.markdown(f"- **重用率:** {pool_stats['reuse_rate']}")
    except:
        st.sidebar.markdown("- **狀態:** 未初始化")
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📥 下載並更新數據", "🔍 篩選已下載數據", "📊 回測系統", 
        "🔔 警報設定", "📲 即時推送", "🔄 背景自動更新", "📋 改進計劃"
    ])
    
    with tab1:
        st.subheader("📥 下載並更新數據 (支援暫停/停止)")
        download_btn = st.button("🔄 下載並篩選強勢股", type="primary", width='stretch')
        
        if download_btn:
            workers = min(4, max(1, max_stocks // 20))
            
            results, updated, failed, qualifies = batch_update(
                stocks_df=stocks_df.head(max_stocks),
                force_update=force_update,
                max_stocks=max_stocks,
                data_period=data_period,
                workers=workers,
                enable_ma20_breakout=enable_ma20,
                enable_ma100_breakout=enable_ma100,
                enable_volume_surge=enable_volume,
                enable_momentum=enable_momentum,
                enable_trend_following=enable_trend,
                enable_rsi=enable_rsi,
                enable_macd=enable_macd,
                min_ret_5d=min_5d_return,
                min_ret_3d=min_3d_return,
                min_volume_ratio=volume_ratio,
                min_price_strength=min_price_strength,
                max_rsi=max_rsi
            )
            
            if results:
                st.success(f"找到 **{len(results)}** 檔符合條件的強勢股！")
                
                results_df = pd.DataFrame(results)
                results_df = results_df.sort_values('5日漲幅%', ascending=False)
                
                display_cols = ['代碼', '名稱', '行業', '最新價', 'MA5', 'MA10', 'MA20', 'MA50', 'MA100',
                               'RSI(14)', 'MACD', '布林帶', '量比', 'Stoch%K', 'Stoch%D', 'ROC(5)', 'ATR%',
                               '5日漲幅%', '3日漲幅%', '20日漲幅%', '價格強度', '站上MA20', '站上MA100']
                
                available_cols = [col for col in display_cols if col in results_df.columns]
                
                st.dataframe(
                    results_df[available_cols].style.format({
                        '最新價': '{:.3f}', 'MA5': '{:.3f}', 'MA10': '{:.3f}', 'MA20': '{:.3f}', 'MA50': '{:.3f}', 'MA100': '{:.3f}',
                        'RSI(14)': '{:.1f}', '量比': '{:.2f}',
                        'Stoch%K': '{:.1f}', 'Stoch%D': '{:.1f}', 'ROC(5)': '{:.2f}%', 'ATR%': '{:.2f}%',
                        '5日漲幅%': '{:.2f}%', '3日漲幅%': '{:.2f}%', '20日漲幅%': '{:.2f}%', '價格強度': '{:.1f}'
                    }).background_gradient(subset=['5日漲幅%', '3日漲幅%', '價格強度'], cmap='RdYlGn'),
                    use_container_width=True, height=500
                )
    
    with tab2:
        st.subheader("🔍 篩選已下載數據")
        filter_btn = st.button("🔍 篩選強勢股", type="primary", width='stretch')
        
        if filter_btn:
            db = DatabaseManager()
            indicators_df = db.get_all_indicators()
            
            if indicators_df.empty:
                st.warning("數據庫中沒有指標數據，請先使用「下載並更新數據」功能")
            else:
                st.markdown(f"**資料庫中的股票數:** {len(indicators_df)} 檔")
                
                filtered = filter_strong_stocks(
                    indicators_df,
                    enable_ma20_breakout=enable_ma20,
                    enable_ma100_breakout=enable_ma100,
                    enable_volume_surge=enable_volume,
                    enable_momentum=enable_momentum,
                    enable_trend_following=enable_trend,
                    enable_rsi=enable_rsi,
                    enable_macd=enable_macd,
                    min_ret_5d=min_5d_return,
                    min_ret_3d=min_3d_return,
                    min_volume_ratio=volume_ratio,
                    min_price_strength=min_price_strength,
                    max_rsi=max_rsi
                )
                
                display_df = format_filtered_results(filtered)
                
                if not display_df.empty:
                    st.success(f"找到 **{len(display_df)}** 檔符合條件的強勢股！")
                    display_df = display_df.sort_values('5日漲幅%', ascending=False)
                    
                    st.dataframe(
                        display_df.style.format({
                            '最新價': '{:.3f}', 'MA5': '{:.3f}', 'MA10': '{:.3f}', 'MA20': '{:.3f}', 'MA50': '{:.3f}', 'MA100': '{:.3f}', 'MA200': '{:.3f}',
                            'RSI(14)': '{:.1f}', '量比': '{:.2f}',
                            'Stoch%K': '{:.1f}', 'Stoch%D': '{:.1f}', 'ROC(5)': '{:.2f}%', 'ATR%': '{:.2f}%',
                            '5日漲幅%': '{:.2f}%', '3日漲幅%': '{:.2f}%', '20日漲幅%': '{:.2f}%', '價格強度': '{:.1f}'
                        }).background_gradient(subset=['5日漲幅%', '3日漲幅%', '價格強度'], cmap='RdYlGn'),
                        use_container_width=True, height=500
                    )
                else:
                    st.warning("沒有符合條件的股票，請調整篩選參數")
    
    with tab3:
        st.subheader("📊 回測系統")
        
        col_bt1, col_bt2 = st.columns(2)
        with col_bt1:
            bt_ticker = st.text_input("回測股票代碼", value="0700.HK")
        with col_bt2:
            bt_strategy = st.selectbox("策略", ["MA_Crossover", "RSI_Overbought_Oversold", "MA20_Trend"])
        
        col_bt3, col_bt4 = st.columns(2)
        with col_bt3:
            bt_capital = st.number_input("初始資金", value=100000.0, step=10000.0)
        with col_bt4:
            bt_position = st.slider("倉位比例", 0.05, 0.5, 0.1)
        
        col_bt5, col_bt6 = st.columns(2)
        with col_bt5:
            bt_stop_loss = st.slider("止損比例", 0.01, 0.2, 0.05)
        with col_bt6:
            bt_take_profit = st.slider("止盈比例", 0.05, 0.5, 0.15)
        
        if st.button("▶️ 執行回測", type="primary", width='stretch'):
            db = DatabaseManager()
            bt_system = BacktestSystem(db, yf_downloader)
            
            try:
                result = bt_system.run_backtest(
                    ticker=bt_ticker, strategy=bt_strategy, initial_capital=bt_capital,
                    position_size=bt_position, stop_loss=bt_stop_loss, take_profit=bt_take_profit
                )
                
                st.success("回測完成！")
                
                col_res1, col_res2, col_res3 = st.columns(3)
                col_res1.metric("總回報", f"{result.get('total_return', 0):.2f}%")
                col_res2.metric("年化回報", f"{result.get('annual_return', 0):.2f}%")
                col_res3.metric("最大回撤", f"-{result.get('max_drawdown', 0):.2f}%")
                
                col_res4, col_res5, col_res6 = st.columns(3)
                col_res4.metric("夏普比率", f"{result.get('sharpe_ratio', 0):.2f}")
                col_res5.metric("勝率", f"{result.get('win_rate', 0):.1f}%")
                col_res6.metric("交易次數", result.get('num_trades', 0))
                
                history = db.get_backtest_results()
                if not history.empty:
                    st.markdown("### 歷史回測結果")
                    st.dataframe(history, use_container_width=True)
                    
            except Exception as e:
                st.error(f"回測失敗: {str(e)}")
    
    with tab4:
        st.subheader("🔔 警報設定")
        
        col_alert1, col_alert2 = st.columns(2)
        with col_alert1:
            alert_ticker = st.text_input("警報股票代碼", value="0700.HK")
        with col_alert2:
            alert_type = st.selectbox("條件類型", ["price_above", "price_below", "rsi_above", "rsi_below", "volume_above"])
        
        alert_value = st.number_input("條件值", value=100.0, step=1.0)
        
        col_alert3, col_alert4 = st.columns(2)
        with col_alert3:
            if st.button("➕ 添加警報", width='stretch'):
                db = DatabaseManager()
                alert_id = db.save_alert({
                    'ticker': alert_ticker,
                    'condition_type': alert_type,
                    'condition_value': alert_value
                })
                st.success(f"警報已添加: {alert_ticker} {alert_type} {alert_value}")
                st.rerun()
        
        with col_alert4:
            if st.button("🗑️ 取消所有警報", type="secondary", width='stretch'):
                db = DatabaseManager()
                count = db.delete_all_alerts()
                if count > 0:
                    st.success(f"已成功取消 {count} 個警報！")
                else:
                    st.info("目前沒有設定任何警報")
                st.rerun()
        
        st.markdown("### 📋 現有警報")
        db = DatabaseManager()
        alerts_df = db.get_alerts()
        
        if not alerts_df.empty:
            alerts_df['創建時間'] = alerts_df['created_at'].apply(lambda x: str(x)[:19] if x else '-')
            alerts_df['狀態'] = alerts_df['is_triggered'].apply(lambda x: '已觸發' if x else '待命中')
            
            st.dataframe(
                alerts_df[['id', 'ticker', 'condition_type', 'condition_value', '狀態', '創建時間']],
                use_container_width=True,
                height=300
            )
        else:
            st.info("暫無警報")
    
    with tab5:
        st.subheader("📲 即時推送配置")
        
        with st.expander("📱 Telegram 配置", expanded=False):
            tg_enabled = st.checkbox("啟用 Telegram", value=config.get('telegram_enabled', False))
            tg_token = st.text_input("Bot Token", value=config.get('telegram_bot_token', ''), type="password")
            tg_chat_id = st.text_input("Chat ID", value=config.get('telegram_chat_id', ''))
            
            if st.button("測試 Telegram"):
                if tg_token and tg_chat_id:
                    provider = TelegramProvider({'bot_token': tg_token, 'chat_id': tg_chat_id})
                    success, msg = provider.test_connection()
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                else:
                    st.warning("請填寫 Bot Token 和 Chat ID")
            
            if st.button("保存 Telegram 配置"):
                config.set('telegram_enabled', tg_enabled)
                config.set('telegram_bot_token', tg_token)
                config.set('telegram_chat_id', tg_chat_id)
                st.success("配置已保存！")
        
        with st.expander("💬 LINE 配置", expanded=False):
            line_enabled = st.checkbox("啟用 LINE", value=config.get('line_enabled', False))
            line_token = st.text_input("Access Token", value=config.get('line_access_token', ''), type="password")
            
            if st.button("測試 LINE"):
                if line_token:
                    provider = LineProvider({'access_token': line_token})
                    success, msg = provider.test_connection()
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
            
            if st.button("保存 LINE 配置"):
                config.set('line_enabled', line_enabled)
                config.set('line_access_token', line_token)
                st.success("配置已保存！")
        
        with st.expander("📞 WhatsApp 配置", expanded=False):
            wa_enabled = st.checkbox("啟用 WhatsApp", value=config.get('whatsapp_enabled', False))
            wa_sid = st.text_input("Account SID", value=config.get('whatsapp_account_sid', ''))
            wa_token = st.text_input("Auth Token", value=config.get('whatsapp_auth_token', ''), type="password")
            wa_from = st.text_input("From Number", value=config.get('whatsapp_from_number', ''))
            
            if st.button("測試 WhatsApp"):
                if wa_sid and wa_token:
                    provider = WhatsAppProvider({'account_sid': wa_sid, 'auth_token': wa_token, 'from_number': wa_from})
                    success, msg = provider.test_connection()
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
            
            if st.button("保存 WhatsApp 配置"):
                config.set('whatsapp_enabled', wa_enabled)
                config.set('whatsapp_account_sid', wa_sid)
                config.set('whatsapp_auth_token', wa_token)
                config.set('whatsapp_from_number', wa_from)
                st.success("配置已保存！")
        
        with st.expander("📧 Email 配置", expanded=False):
            email_enabled = st.checkbox("啟用 Email", value=config.get('email_enabled', False))
            email_server = st.text_input("SMTP 伺服器", value=config.get('email_smtp_server', 'smtp.gmail.com'))
            email_sender = st.text_input("發送者郵箱", value=config.get('email_sender', ''))
            email_password = st.text_input("郵箱密碼", value=config.get('email_password', ''), type="password")
            email_recipients = st.text_input("收件人", value=config.get('email_recipients', ''))
            
            if st.button("測試 Email"):
                if email_sender and email_password:
                    provider = EmailProvider({'smtp_server': email_server, 'sender': email_sender, 'password': email_password, 'recipients': email_recipients})
                    success, msg = provider.test_connection()
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
            
            if st.button("保存 Email 配置"):
                config.set('email_enabled', email_enabled)
                config.set('email_smtp_server', email_server)
                config.set('email_sender', email_sender)
                config.set('email_password', email_password)
                config.set('email_recipients', email_recipients)
                st.success("配置已保存！")
    
    with tab6:
        st.subheader("🔄 背景自動更新 (靜默模式)")
        
        st.info("💡 **背景自動更新模式**：每天固定時間自動更新，不發送即時通知，避免打擾。")
        
        db = DatabaseManager()
        
        col_auto1, col_auto2 = st.columns([3, 1])
        with col_auto1:
            auto_update_enabled = st.checkbox("✅ 啟用背景自動更新", value=config.get('auto_update_enabled', False))
        with col_auto2:
            update_time = config.get('auto_update_time', '12:00')
            next_run = None
            try:
                hour, minute = map(int, update_time.split(':'))
                now = datetime.now()
                next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if now >= next_run:
                    next_run = next_run + timedelta(days=1)
            except:
                next_run = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(days=1)
            
            st.markdown(f"**下次執行:**<br>{next_run.strftime('%Y-%m-%d %H:%M')}" if next_run else "未設定", unsafe_allow_html=True)
        
        if auto_update_enabled and not (background_task_manager and background_task_manager.is_running):
            config.set('auto_update_enabled', True)
            
            if background_task_manager is None:
                background_task_manager = BackgroundTaskManager()
            
            background_task_manager.initialize(db, stocks_df, yf_downloader, notification_manager=None)
            
            update_time = config.get('auto_update_time', '12:00')
            auto_max_stocks = config.get('auto_update_max_stocks', 676)
            auto_outdated_days = config.get('auto_update_outdated_days', 1)
            
            background_task_manager.start(update_time=update_time, max_stocks=auto_max_stocks, outdated_days=auto_outdated_days)
            st.success(f"背景自動更新已啟動！每天 {update_time} 執行 (靜默模式，不發送通知)")
            st.rerun()
        
        if not auto_update_enabled and background_task_manager and background_task_manager.is_running:
            background_task_manager.stop()
            config.set('auto_update_enabled', False)
            st.info("背景自動更新已停止")
            st.rerun()
        
        st.markdown("### ⚙️ 更新配置")
        
        col_bg1, col_bg2, col_bg3 = st.columns(3)
        
        with col_bg1:
            update_time = st.text_input("每日更新時間 (時:分)", value=config.get('auto_update_time', '12:00'), help="例如: 12:00 表示每天中午12點執行")
        
        with col_bg2:
            auto_outdated_days = st.number_input("過期天數", min_value=1, max_value=30, value=config.get('auto_update_outdated_days', 1))
        
        with col_bg3:
            auto_max_stocks = st.number_input("每次最大更新數量", min_value=10, max_value=min(676, total_stocks), value=min(config.get('auto_update_max_stocks', 676), total_stocks), step=10)
            st.caption(f"⚠️ 最大可選: {total_stocks} 檔")
        
        try:
            hour, minute = map(int, update_time.split(':'))
            now = datetime.now()
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if now >= next_run:
                next_run = next_run + timedelta(days=1)
            
            st.info(f"📅 下次自動更新時間: **{next_run.strftime('%Y-%m-%d %H:%M')}**")
        except:
            st.warning("時間格式錯誤，請使用 HH:MM 格式")
        
        if st.button("💾 保存配置"):
            config.set('auto_update_time', update_time)
            config.set('auto_update_max_stocks', auto_max_stocks)
            config.set('auto_update_outdated_days', auto_outdated_days)
            st.success("配置已保存！")
            
            if background_task_manager and background_task_manager.is_running:
                background_task_manager.stop()
                time.sleep(1)
                background_task_manager.start(update_time=update_time, max_stocks=auto_max_stocks, outdated_days=auto_outdated_days)
                st.success("配置已保存，任務已重啟！")
            
            st.rerun()
        
        st.markdown("---")
        
        st.markdown("### 🎮 任務控制")
        
        if background_task_manager:
            bg_status = background_task_manager.get_status()
        else:
            bg_status = {
                'is_running': False,
                'auto_update': {
                    'running': False,
                    'last_run': None,
                    'total_runs': 0,
                    'total_updates': 0,
                    'total_failures': 0,
                    'status': 'stopped'
                }
            }
        
        col_ctrl1, col_ctrl2, col_ctrl3 = st.columns(3)
        
        with col_ctrl1:
            if bg_status['is_running']:
                if st.button("⏹️ 停止背景任務", type="primary", width='stretch'):
                    if background_task_manager:
                        background_task_manager.stop()
                        config.set('auto_update_enabled', False)
                        st.success("背景任務已停止！")
                        st.rerun()
            else:
                if st.button("▶️ 啟動背景任務", width='stretch'):
                    if background_task_manager is None:
                        background_task_manager = BackgroundTaskManager()
                        background_task_manager.initialize(db, stocks_df, yf_downloader, notification_manager=None)
                    
                    background_task_manager.start(update_time=update_time, max_stocks=auto_max_stocks, outdated_days=auto_outdated_days)
                    config.set('auto_update_enabled', True)
                    st.success(f"背景任務已啟動！將在每天 {update_time} 執行")
                    st.rerun()
        
        with col_ctrl2:
            if bg_status['auto_update'].get('running', False):
                if st.button("❌ 取消當前任務", width='stretch'):
                    if background_task_manager:
                        success = background_task_manager.cancel_current_task()
                        if success:
                            st.success("已取消當前任務！")
                        else:
                            st.info("沒有正在運行的任務")
                        st.rerun()
            else:
                if st.button("🚀 立即執行一次", width='stretch'):
                    if background_task_manager is None:
                        background_task_manager = BackgroundTaskManager()
                        background_task_manager.initialize(db, stocks_df, yf_downloader, notification_manager=None)
                    
                    success, msg = background_task_manager.run_now(max_stocks=auto_max_stocks, outdated_days=auto_outdated_days, notify=False)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
        
        with col_ctrl3:
            if st.button("🔄 刷新狀態", width='stretch'):
                st.rerun()
        
        st.markdown("---")
        st.markdown("### 📊 處理進度")
        
        if background_task_manager:
            progress = background_task_manager.get_progress()
            
            progress_bar = st.progress(progress.get('percentage', 0) / 100)
            
            col_progress1, col_progress2, col_progress3, col_progress4 = st.columns(4)
            
            current_stock = progress.get('current_stock', '-')
            current_name = progress.get('current_stock_name', '')
            if current_name and current_name != current_stock:
                current_stock = f"{current_stock} ({current_name})"
            
            col_progress1.markdown(f"""
            <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 5px;">
                <div style="font-size: 12px; color: #666;">當前股票</div>
                <div style="font-size: 16px; font-weight: bold; color: #667eea;">{current_stock}</div>
            </div>
            """, unsafe_allow_html=True)
            
            col_progress2.markdown(f"""
            <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 5px;">
                <div style="font-size: 12px; color: #666;">已處理</div>
                <div style="font-size: 16px; font-weight: bold;">{progress.get('processed', 0)} / {progress.get('total', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
            
            col_progress3.markdown(f"""
            <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 5px;">
                <div style="font-size: 12px; color: #666;">進度</div>
                <div style="font-size: 16px; font-weight: bold; color: #28a745;">{progress.get('percentage', 0):.1f}%</div>
            </div>
            """, unsafe_allow_html=True)
            
            col_progress4.markdown(f"""
            <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 5px;">
                <div style="font-size: 12px; color: #666;">預計剩餘</div>
                <div style="font-size: 16px; font-weight: bold; color: #fd7e14;">{progress.get('eta_seconds', 0):.0f}秒</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"""
            | 狀態 | 已更新 | 失敗 | 強勢股 | 處理速度 |
            |:----:|:------:|:----:|:------:|:--------:|
            | {'運行中' if progress.get('is_running') else '已完成'} | {progress.get('updated', 0)} | {progress.get('failed', 0)} | {progress.get('qualified', 0)} | {progress.get('speed', 0):.1f} 檔/秒 |
            """)
            
            logs = progress.get('logs', [])
            if logs:
                st.markdown("#### 📜 實時日誌")
                log_text = ""
                for log in logs[-15:]:
                    log_text += f"| {log['time']} | {log['icon']} | {log['message']} |\n"
                st.markdown(f"""
                | 時間 | 狀態 | 訊息 |
                |------|:----:|------|
                {log_text}
                """)
        else:
            st.info("背景任務管理器未初始化")
        
        st.markdown("---")
        
        st.markdown("### 📊 任務狀態")
        
        status_icons = {
            'running': '🟢 運行中',
            'scheduled': '🟡 已調度',
            'completed': '✅ 完成',
            'stopped': '🔴 已停止',
            'cancelled': '🟠 已取消',
            'error': '❌ 錯誤',
            'idle': '⚪ 空閒'
        }
        
        auto_update_status = bg_status.get('auto_update', {})
        status_icon = status_icons.get(auto_update_status.get('status', 'stopped'), '🔴 已停止')
        is_running = bg_status.get('is_running', False)
        is_task_running = auto_update_status.get('running', False)
        last_run = auto_update_status.get('last_run')
        total_runs = auto_update_status.get('total_runs', 0)
        total_updates = auto_update_status.get('total_updates', 0)
        total_failures = auto_update_status.get('total_failures', 0)
        
        st.markdown(f"""
        - **調度器狀態:** {status_icon} (運行中: {is_running})
        - **當前任務:** {'運行中' if is_task_running else '空閒'}
        - **總運行次數:** {total_runs}
        - **總更新股票:** {total_updates}
        - **失敗次數:** {total_failures}
        - **更新模式:** 📅 每天 {update_time} 執行 (預設不啟用)
        - **最後運行:** {str(last_run)[:19] if last_run else '尚未運行'}
        - **通知模式:** 🚫 靜默 (不發送即時推送)
        """)
        
        st.markdown("### 📜 任務歷史")
        tasks_df = db.get_all_background_tasks(50)
        if not tasks_df.empty:
            tasks_df['狀態'] = tasks_df['status'].map(status_icons).fillna(tasks_df['status'])
            tasks_df['運行時間'] = tasks_df['last_run'].apply(lambda x: str(x)[:19] if x else '-')
            tasks_df['下次運行'] = tasks_df['next_run'].apply(lambda x: str(x)[:19] if x else '-')
            tasks_df['創建時間'] = tasks_df['created_at'].apply(lambda x: str(x)[:19] if x else '-')
            
            st.dataframe(
                tasks_df[['task_name', '狀態', '運行時間', '下次運行', 'error_message', '創建時間']],
                use_container_width=True,
                height=300
            )
            
            if st.button("🗑️ 清空任務歷史"):
                count = db.clear_background_tasks()
                st.success(f"已清空 {count} 條任務記錄")
                st.rerun()
        else:
            st.info("暫無任務歷史")
    
    with tab7:
        st.subheader("📋 改進計劃")
        
        st.markdown("### ✅ 短期改進 (1-2週)")
        for item in IMPROVEMENT_PLAN['short_term']['items']:
            st.markdown(f"""
            <div style="padding: 10px; margin: 5px 0; background-color: #d4edda; border-radius: 5px; border-left: 4px solid #28a745;">
                <strong>✅ {item['task']}</strong><br>
                <small style="color: #555;">{item['desc']}</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("### ✅ 中期改進 (1-2個月)")
        for item in IMPROVEMENT_PLAN['mid_term']['items']:
            st.markdown(f"""
            <div style="padding: 10px; margin: 5px 0; background-color: #d4edda; border-radius: 5px; border-left: 4px solid #28a745;">
                <strong>✅ {item['task']}</strong><br>
                <small style="color: #555;">{item['desc']}</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("### ✅ 長期改進 (3-6個月) - 已完成")
        for item in IMPROVEMENT_PLAN['long_term']['items']:
            st.markdown(f"""
            <div style="padding: 10px; margin: 5px 0; background-color: #d4edda; border-radius: 5px; border-left: 4px solid #28a745;">
                <strong>✅ {item['task']}</strong><br>
                <small style="color: #555;">{item['desc']}</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div style="background-color: #f0f2f6; padding: 15px; border-radius: 10px; margin-top: 20px;">
            <h4>📦 版本資訊</h4>
            <table style="width: 100%; font-size: 14px;">
                <tr><td style="padding: 5px;"><strong>應用版本:</strong></td><td>{APP_VERSION}</td></tr>
                <tr><td style="padding: 5px;"><strong>綱要版本:</strong></td><td>{SCHEMA_VERSION}</td></tr>
                <tr><td style="padding: 5px;"><strong>最後更新:</strong></td><td>{APP_VERSION_DATE}</td></tr>
                <tr><td style="padding: 5px;"><strong>作者:</strong></td><td>{APP_AUTHOR}</td></tr>
                <tr><td style="padding: 5px;"><strong>已完成項目:</strong></td><td>18/18 (100%)</td></tr>
                <tr><td style="padding: 5px;"><strong>股票總數:</strong></td><td>{total_stocks} 檔</td></tr>
                <tr><td style="padding: 5px;"><strong>本次修復:</strong></td><td>NameError: 'initialize_globals' 修復</td></tr>
            </table>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align: center; color: #666; font-size: 12px;">
        <strong>港股強勢股篩選器 V{APP_VERSION}</strong> | SQLite並發連接修復版 (連接池 + WAL + 重試)<br>
        ✅ 所有短期改進已完成 | ✅ 所有中期改進已完成 | ✅ 所有長期改進已完成<br>
        📊 股票總數: {total_stocks} 檔 | 🎯 核心邏輯: MA20 + MA100 (預設啟用)<br>
        📲 即時推送: Telegram | LINE | WhatsApp | Email | Webhook | 🔄 背景自動更新 (每天 {config.get('auto_update_time', '12:00')} 執行，預設不啟用) | 🔔 警報設定 (支援取消所有警報) | 🗄️ SQLite並發優化 (連接池 + 重試 + 指數退避)
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

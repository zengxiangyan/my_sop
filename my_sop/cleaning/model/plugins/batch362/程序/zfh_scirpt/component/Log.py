import logging
import logging.config
import os
import threading
import warnings


# 单例实例化
class Log:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, name, level=logging.INFO):
        # 使用双重检查锁定模式确保线程安全
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = init_logger(name, level)
        return cls._instance


class ignore_level(logging.Filter):
    def __init__(self, max_level):
        super().__init__()
        self.max_level = max_level

    def filter(self, record):
        return record.levelno != self.max_level


def init_logger(name, level):
    # 确保 runtime 文件夹存在
    runtime_dir = '../runtime'
    log_file = runtime_dir + '/app.log'
    error_file = runtime_dir + '/error.log'
    if not os.path.exists(runtime_dir):
        os.makedirs(runtime_dir)

    log_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
        },
        'filters': {
            'ignore_warning': {
                '()': ignore_level,
                'max_level': logging.WARNING,
            },
        },
        'handlers': {
            # 输出 INFO
            'console': {
                'class': 'logging.StreamHandler',
                'level': level,
                'formatter': 'simple',
                'filters': ['ignore_warning'],
                'stream': 'ext://sys.stdout',
            },
            # 输出 INFO DEBUG
            'file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'simple',
                'filters': ['ignore_warning'],
                'filename': log_file,
                "encoding": "utf-8",
                'mode': 'w',
            },
            # 输出 ERROR WARNING
            'error': {
                'class': 'logging.FileHandler',
                'level': 'WARNING',
                'formatter': 'simple',
                'filename': error_file,
                "encoding": "utf-8",
                'mode': 'w',
            }
        },
        'loggers': {
            name: {
                'handlers': ['console', 'file', 'error'],
                'level': 'DEBUG',
                'propagate': False,
            },
        }
    }

    logging.config.dictConfig(log_config)
    logger = logging.getLogger(name)

    # 将warning重定向到log组件上
    def log_warning(message, category, filename, lineno, file=None, line=None):
        logger.warning(f"[Warning] {filename}:{lineno} - {message}")

    warnings.showwarning = log_warning
    return logger


import logging
import os
from logging.handlers import TimedRotatingFileHandler

# 配置log
formatter = logging.Formatter(
    '[%(asctime)s][%(levelname)s][%(filename)s][%(lineno)d][-][%(thread)d]=[%(message)s]')
handler = logging.StreamHandler()
handler.setFormatter(formatter)

# 确保日志目录存在
log_dir = 'log'
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

app_logger = logging.getLogger('lottery')
app_handler = TimedRotatingFileHandler(
    log_dir + '/lottery_activity.log', when='midnight', backupCount=7, encoding='utf8')
app_handler.setFormatter(formatter)
app_logger.addHandler(app_handler)
app_logger.addHandler(handler)
app_logger.setLevel(logging.DEBUG)

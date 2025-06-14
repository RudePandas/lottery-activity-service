
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List
from telegram import Bot

from mysql.aio import aio_mysql
from helper import *
from app.lottery_activity_handler.logger_handler import app_logger

class ActivityStatus(Enum):
    """活动状态枚举"""
    PENDING = 1
    ACTIVE = 2
    KILLED = 3
    ENDED = 4


class ConditionType(Enum):
    """条件类型枚举"""
    JOIN_GROUP = "join_group"
    JOIN_CHANNEL = "join_channel"
    FOLLOW_BOT = "follow_bot"
    SPEECH_COUNT = "speech_count"


class ConditionStatus(Enum):
    """条件验证状态"""
    PENDING = "pending"
    VERIFIED = "verified"
    FAILED = "failed"


@dataclass
class Condition:
    """活动参与条件"""
    type: ConditionType
    target_id: str  # 群组/频道/机器人的ID/发言的群组(用,连接)
    target_id_link: str  # 群组/频道/机器人的链接/发言次数
    button_name: str
    name: str
    status: ConditionStatus = ConditionStatus.PENDING

    def to_dict(self) -> Dict:
        return {
            'type': self.type.value,
            'target_id': self.target_id,
            'target_id_link': self.target_id_link,
            'status': self.status.value,
            'button_name': self.button_name,
            'name': self.name
        }


@dataclass
class Price:
    """奖品"""
    prize_name: str
    prize_content: str
    prize_count: int

@dataclass
class ActivityReply:
    """固定文案"""
    id: int
    reply_type: str
    content: str
    media: str
    sys_user_id: int
    buttons: List[str] = field(default_factory=list)

@dataclass
class ActivityUser:
    """抽奖用户"""
    id: int
    user_name: str
    user_id: int
    full_name: str
    condition_status: int
    winning_status: int
    winning_content: str
    activity_id: int
    prize_level: int

@dataclass
class Activity:
    """抽奖活动"""
    id: str
    name: str
    start_time: datetime
    end_time: datetime
    prices: List[Price]
    sys_user_id: int
    scope: str
    checked: int
    activity_users: List[ActivityUser] = field(default_factory=list)
    conditions: List[Condition] = field(default_factory=list)
    activity_status: ActivityStatus = ActivityStatus.PENDING
    activities_reply: Dict[int, ActivityReply] = field(default_factory=dict)
    participants: List[str] = field(default_factory=list)
    
    
    def is_active(self) -> bool:
        """检查活动是否在进行中"""
        now = datetime.now()
        return self.start_time <= now <= self.end_time and self.activity_status == ActivityStatus.ACTIVE.value
    
    def should_start(self) -> bool:
        """检查是否应该开始活动"""
        return datetime.now() >= self.start_time and self.activity_status == ActivityStatus.PENDING.value
    
    def should_check(self) -> bool:
        """检查是否应该做活动结束前半小时检查"""
        now = datetime.now()
        return int((self.end_time-self.start_time).seconds) >= 1800 and int((self.end_time-now).seconds) <= 1800 and self.activity_status == ActivityStatus.ACTIVE.value
    
    def should_end(self) -> bool:
        """检查是否应该结束活动"""
        return datetime.now() >= self.end_time and self.activity_status == ActivityStatus.ACTIVE.value

    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'name': self.name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'activity_status': self.activity_status.value,
            'activities_reply': self.activities_reply,
            'conditions': [c.to_dict() for c in self.conditions],
            'participants': self.participants,
            'sys_user_id': self.sys_user_id,
            'scope': self.scope,
            'checked': self.checked
        }


class LotteryBot():
    """抽奖机器人"""
    def __init__(self):
        pass

    @staticmethod
    async def is_lottery_bot(bot_):
        bot, created_by, first_name, language = bot_
        res = await aio_mysql.execute_sql(f"SELECT * FROM bot_tokens WHERE bot_id={bot.id} AND is_activity=1")
        app_logger.info(f"获取是否抽奖活动机器人: bot: {bot}, created_by: {created_by}, res_msg: {res.msg}, res_data: {res.data}")
        if res.data:
            return True
        return False
    
    async def get_lottery_bot(sys_user_id):
        res = await aio_mysql.execute_sql(f"SELECT * FROM bot_tokens WHERE created_by={sys_user_id} AND is_activity=1")
        app_logger.info(f"获取抽奖活动机器人: sys_user_id: {sys_user_id}, res_msg: {res.msg}, res_data: {res.data}")
        if res.data:
            return res.data[0]
        return False
    
    @staticmethod
    async def get_first_bot(group_id, tag, sys_user_id) -> Bot:
        bot = get_first_group_bot(group_id, sys_user_id) if tag == "join_group" else get_first_channel_bot(group_id, sys_user_id)
        bot_token = bot[0]["token"]
        bot = Bot(bot_token)
        return bot
    
    @staticmethod
    async def get_start_command(bot_):
        bot, created_by, first_name, language = bot_
        res = await aio_mysql.execute_sql(f"SELECT * FROM bot_tokens WHERE bot_id={bot.id} AND is_activity=1 AND created_by={created_by}")
        if res.data:
            return res.data[0]["activity_word"]
        return False
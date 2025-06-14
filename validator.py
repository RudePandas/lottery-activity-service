from abc import ABC, abstractmethod
from typing import Dict
from mysql.aio import aio_mysql
from helper import *
from app.lottery_activity_handler.data_class import ConditionType, LotteryBot
from app.lottery_activity_handler.data_repository import IDataRepository
from app.lottery_activity_handler.logger_handler import app_logger


class IConditionValidator(ABC):
    """条件验证器接口"""
    
    @abstractmethod
    async def validate(self, user_id: str, *args) -> bool:
        """验证用户是否满足条件"""
        pass

class JoinGroupValidator(IConditionValidator):
    """加群条件验证器"""
    
    async def validate(self, user_id: str, *args) -> bool:
        try:
            condition, bot, sys_user_id = args
            app_logger.info(f"加群组条件验证 参数: target_id: {condition.target_id}, type: {condition.type.value}, sys_user_id: {sys_user_id}")
            bot = await LotteryBot.get_first_bot(condition.target_id, condition.type.value, sys_user_id)
            member = await bot.get_chat_member(condition.target_id, user_id)
            app_logger.info(f"加群组条件验证 结果: target_id: {condition.target_id}, type: {condition.type.value}, sys_user_id: {sys_user_id}, res: {member}")
            return member.status in ['member', 'administrator', 'creator']
        except Exception as e:
            app_logger.error(f"验证群组条件失败: {e}", exc_info=True)
            return False


class JoinChannelValidator(IConditionValidator):
    """加频道条件验证器"""
    
    async def validate(self, user_id: str, *args) -> bool:
        try:
            condition, bot, sys_user_id = args
            app_logger.info(f"加频道条件验证 参数: target_id: {condition.target_id}, type: {condition.type.value}, sys_user_id: {sys_user_id}")
            bot = await LotteryBot.get_first_bot(condition.target_id, condition.type.value, sys_user_id)
            member = await bot.get_chat_member(condition.target_id, user_id)
            app_logger.info(f"加频道条件验证 结果: target_id: {condition.target_id}, type: {condition.type.value}, sys_user_id: {sys_user_id}, res: {member}")
            return member.status in ['member', 'administrator', 'creator']
        except Exception as e:
            app_logger.error(f"验证加频道条件失败: {e}", exc_info=True)
            return False


class FollowBotValidator(IConditionValidator):
    """关注机器人条件验证器"""
    
    async def validate(self, user_id: str, *args) -> bool:
        try:
            condition, bot, sys_user_id = args
            app_logger.info(f"关注机器人条件验证 参数: target_id: {condition.target_id}, type: {condition.type.value}, sys_user_id: {sys_user_id}, tg_user_id: {user_id}")
            res = check_users_follow_bots(condition.target_id, user_id, sys_user_id)
            app_logger.info(f"关注机器人条件验证 结果: target_id: {condition.target_id}, type: {condition.type.value}, sys_user_id: {sys_user_id}, tg_user_id: {user_id}, res: {res}")
            if res:return True
        except Exception as e:
            app_logger.error(f"验证关注机器人条件失败: {e}", exc_info=True)
            return False

class SpeechCountValidator(IConditionValidator):
    """发言次数条件验证器"""
    
    async def validate(self, user_id: str, *args) -> bool:
        try:
            activity = args[0]
            for condition in activity.conditions:
                if condition.type.value == "speech_count":
                    groups = condition.target_id.split(',')
                    for group in groups:
                        activities_reply_data = await aio_mysql.execute_sql(f"SELECT * FROM chat_messages_logs WHERE user_id={user_id} AND chat_id={group} AND created_at>'{activity.start_time}' AND created_at<'{activity.end_time}'")
                        if activities_reply_data.code == 200 and activities_reply_data.data:
                            times = len(activities_reply_data.data)
                        else:
                            times = 0
                        app_logger.info(f"这个群发言次数验证: group: {group}, start_time: '{activity.start_time}', end_time: '{activity.end_time}', tg_user_id: {user_id}, 目标次数: {condition.target_id_link}, 当前次数: {times}")
                        if times < int(condition.target_id_link):
                            return False
            app_logger.info(f"群发言次数验证 结果: groups: '{groups}', start_time: '{activity.start_time}', end_time: '{activity.end_time}', tg_user_id: {user_id}, 目标次数: {condition.target_id_link}, 当前次数: {times}")
            return True
        except Exception as e:
            app_logger.error(f"验证发言次数条件失败: {e}", exc_info=True)
            return False
        

class ConditionValidatorFactory:
    """条件验证器工厂"""
    
    _validators = {
        ConditionType.JOIN_GROUP: JoinGroupValidator(),
        ConditionType.JOIN_CHANNEL: JoinChannelValidator(),
        ConditionType.FOLLOW_BOT: FollowBotValidator(),
        ConditionType.SPEECH_COUNT: SpeechCountValidator(),
    }
    
    @classmethod
    def get_validator(cls, condition_type: ConditionType) -> IConditionValidator:
        return cls._validators.get(condition_type)
    
    async def validate_user_conditions(self, repository: IDataRepository, user_id: str, activity_id: str, bot, sys_user_id) -> Dict:
        """验证用户条件完成情况"""
        try:
            activity = await repository.get_activity_by_id(activity_id)
            app_logger.info(f"验证用户条件完成情况 参数 user_id: {user_id}, activity_id: {activity_id}")
            if not activity:
                app_logger.info(f"验证用户条件完成情况 活动不存在 user_id: {user_id}, activity_id: {activity_id}")
                return {"error": "活动不存在"}
            
            results = {}
            all_verified = True
            
            for condition in activity.conditions:
                validator = ConditionValidatorFactory.get_validator(condition.type)
                if validator and condition.type.value != "speech_count":
                    is_verified = await validator.validate(user_id, condition, bot, sys_user_id)
                    results[condition.type.value] = {
                        "type": condition.type.value,
                        "button_name": condition.button_name,
                        "verified": is_verified
                    }
                    if not is_verified:
                        all_verified = False
                else:
                    is_verified = await validator.validate(user_id, activity)
                    results[condition.type.value] = {
                        "type": condition.type.value,
                        "button_name": condition.button_name,
                        "verified": is_verified
                    }
                    if not is_verified:
                        all_verified = False
            
            if all_verified:
                res_sql = await repository.update_activity_detail(activity.id, user_id, 1)
            else:
                res_sql = await repository.update_activity_detail(activity.id, user_id, 0)
            app_logger.info(f"验证用户条件完成情况 结果: user_id: {user_id}, all_verified: {all_verified}, conditions: {results}")
            return {
                "all_verified": all_verified,
                "conditions": results
            }
        except Exception as e:
            app_logger.error(f"验证用户条件完成情况 异常: {e}", exc_info=True)
            return {"error": str(e)}

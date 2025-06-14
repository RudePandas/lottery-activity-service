import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from mysql.aio import aio_mysql
from helper import *
from app.lottery_activity_handler.data_class import Activity, ActivityReply, Condition, ConditionType, Price, ActivityUser, ActivityStatus
from sdk.dingding import DingTalk
from app.lottery_activity_handler.logger_handler import app_logger


class IDataRepository(ABC):
    """数据仓储接口"""
    
    @abstractmethod
    async def get_activity_by_id(self, activity_id: str) -> Optional[Activity]:
        pass
    
    @abstractmethod
    async def get_all_activities(self, activity_id: int) -> List[Activity]:
        pass
    
    @abstractmethod
    async def set_activity_status(self, activity_id: int, activity_status: int):
        pass
    
    @abstractmethod
    async def save_activity_detail(self, activity_id: int, message: dict) -> None:
        pass
    
    @abstractmethod
    async def update_activity_detail(self, activity_id: int, user_id: int, condition_status: int) -> None:
        pass
    
    @abstractmethod
    async def update_prize_user(self, prize_content: str, sql_id: int, prize_level: int) -> None:
        pass
    
    @abstractmethod
    async def get_groups_by_tag(self, tag, sys_user_id) -> list:
        pass
    
    @abstractmethod
    async def get_user_participation(self, user_id: str, activity_id: str) -> Dict:
        pass
    
    @abstractmethod
    async def save_user_participation(self, user_id: str, activity_id: str, data: Dict) -> None:
        pass
    
    @abstractmethod
    async def update_activity_checked(self, activity_id: str, checked: int) -> None:
        pass
    
    @abstractmethod
    async def get_winning_user(self, activity_id: str) -> list:
        pass
    
    @abstractmethod
    async def get_finish_conditions_user(self, activity_id: str, tg_user_id) -> list:
        pass
    
    @abstractmethod
    async def get_close_activity_by_id(self, activity_id: str) -> Optional[Activity]:
        pass


class InMemoryRepository(IDataRepository):
    """内存数据仓储实现"""
    
    def __init__(self):
        self.activities: Dict[str, Activity] = {}
        self.user_participations: Dict[str, Dict] = {}
    
    async def get_activity_by_id(self, activity_id: str) -> Optional[Activity]:
        app_logger.info(f"id获取活动: activity_id: {activity_id}")
        activities = await self.get_all_activities()
        for activity in activities:
            if activity.id == int(activity_id):
                return activity
        return None
    
    async def get_all_reply(self, sys_user_id):
        try:
            activities_reply_data = await aio_mysql.execute_sql(f"SELECT * FROM activity_reply WHERE sys_user_id={sys_user_id}")
            activities_reply = {}
            if activities_reply_data.data:
                for activity_reply in activities_reply_data.data:
                    activities_reply[activity_reply["reply_type"]]=ActivityReply(
                        id=activity_reply["id"],
                        reply_type=activity_reply["reply_type"],
                        content=activity_reply["content"],
                        buttons=json.loads(activity_reply["buttons"]) if activity_reply["buttons"] else [],
                        media=activity_reply["media"],
                        sys_user_id=activity_reply["sys_user_id"],
                    )
            return activities_reply
        except Exception as e:
            app_logger.error(f"获取用户恢复模板异常: {e}", exc_info= True)
    
    async def get_all_activities(self, activity_id=0) -> List[Activity]:
        try:
            # 动态构建 WHERE 条件
            where_conditions = []

            if not activity_id:
                where_conditions.extend([
                    f"u.activity_status != {ActivityStatus.ENDED.value}",
                    f"u.activity_status != {ActivityStatus.KILLED.value}",
                    f"u.deleted_at IS NULL"
                ])
            else:
                where_conditions.append(f"u.id = {activity_id}")
                
            # 拼接完整 SQL
            sql = f"""SELECT 
                u.id,
                u.name,
                u.start_time,
                u.end_time,
                u.activity_status,
                u.sys_user_id,
                u.prizes,
                u.conditions,
                u.scope,
                u.checked,
                (
                    SELECT JSON_ARRAYAGG(
                        JSON_OBJECT('id', o.id, 'user_name', o.user_name, 'user_id', o.user_id, 'full_name', o.full_name, 'condition_status', o.condition_status, 'winning_status', o.winning_status, 'winning_content', o.winning_content, 'activity_id', o.activity_id, 'prize_level', o.prize_level)
                    )
                    FROM activity_user o 
                    WHERE o.activity_id = u.id
                ) as users
            FROM activity_list u 
            WHERE {' AND '.join(where_conditions)}"""
            activities_data = await aio_mysql.execute_sql(sql)
            
            app_logger.info(f"获取所有抽奖活动 res_sql: {activities_data.msg}")
            
            activities = []
            if activities_data.data:
                for activity_data in activities_data.data:
                    activities_reply = await self.get_all_reply(activity_data["sys_user_id"])
                    conditions_data = json.loads(activity_data["conditions"])
                    prices_data = json.loads(activity_data["prizes"])
                    users = json.loads(activity_data["users"]) if activity_data["users"] else []
                    
                    # 条件
                    conditions = [
                        Condition(
                            type=ConditionType(c["type"]),
                            target_id=c["target_id"],
                            target_id_link=c["target_id_link"],
                            name=c["name"],
                            button_name=c["button_name"]
                        )
                        for c in conditions_data
                    ]
                    
                    # 奖品
                    prices = [
                        Price(
                            prize_name=p["prize_name"],
                            prize_content=p["prize_content"],
                            prize_count=p["prize_count"]
                        )
                        for p in prices_data
                    ]
                    
                    # 参与用户
                    activity_users = [
                        ActivityUser(
                            id=p["id"],
                            user_name=p["user_name"],
                            full_name=p["full_name"],
                            user_id=p["user_id"],
                            condition_status=p["condition_status"],
                            winning_status=p["winning_status"],
                            winning_content=p["winning_content"],
                            activity_id=p["activity_id"],
                            prize_level=p["prize_level"]
                        )
                        for p in users
                    ]
                    
                    # 活动
                    activities.append(
                        Activity(
                            id=activity_data["id"],
                            name=activity_data["name"],
                            start_time=activity_data["start_time"],
                            end_time=activity_data["end_time"],
                            scope=activity_data["scope"],
                            checked=activity_data["checked"],
                            conditions=conditions,
                            activities_reply=activities_reply,
                            activity_users=activity_users,
                            prices=prices,
                            activity_status=activity_data["activity_status"],
                            sys_user_id = activity_data["sys_user_id"]
                        )
                    )
            app_logger.info(f"获取所有抽奖活动: actyvity 共有：{len(activities)}个")
            return activities
        except Exception as e:
            app_logger.error(f"获取所有抽奖活动结果异常: {e}", exc_info=True)
            async with DingTalk() as fetcher:
                await fetcher.ding_talk_waring(f"{e}")
    
    async def set_activity_status(self, activity_id: int, activity_status: int) -> None:
        res_sql = await aio_mysql.execute_sql(f"UPDATE activity_list SET activity_status = {activity_status} WHERE id={activity_id}")
        app_logger.info(f"设置活动状态 activity_id: {activity_id}, activity_status: {activity_status}, res_sql: {res_sql.msg}")
    
    async def update_activity_detail(self, activity_id: int, user_id: int, condition_status: int) -> None:
        res_sql = await aio_mysql.execute_sql(f"UPDATE activity_user SET condition_status = {condition_status} WHERE activity_id={activity_id} AND user_id={user_id}")
        app_logger.info(f"更新活动用户条件状态 activity_id: {activity_id}, user_id: {user_id}, condition_status: {condition_status}, res_sql: {res_sql.msg}")
        
    async def update_prize_user(self, prize_content: str, sql_id: int, prize_level: int) -> None:
        res_sql = await aio_mysql.execute_sql(f"UPDATE activity_user SET winning_status = 1, winning_content = '{prize_content}', prize_level={prize_level} WHERE id={sql_id}")
        app_logger.info(f"更新活动用户奖品状态内容 sql_id: {sql_id}, prize_content: {prize_content}, prize_level: {prize_level}, res_sql: {res_sql.msg}")
        
    async def save_activity_detail(self, activity_id: int, message: dict) -> None:
        try:
            from_data = message.get("from", {})
            tg_user_id = from_data.get("id")
            user_name = from_data.get("username")
            full_name = f"{from_data['first_name']}{from_data['last_name']}" if from_data.get(
                'last_name') else from_data.get('first_name')
            app_logger.info(f"新参与活动用户保存 参数: activity_id: {activity_id}, tg_user_id: {tg_user_id}, user_name: {user_name}")
            res = await aio_mysql.execute_sql(f"INSERT INTO activity_user(user_id, user_name, full_name, activity_id) \
                VALUES({tg_user_id}, '{user_name}', '{full_name}', {activity_id})")
            app_logger.info(f"新参与活动用户 res_sql: {res.msg}, activity_id: {activity_id}")
            return res
        except Exception as e:
            app_logger.error(f"新参与活动用户保存异常: {e}", exc_info=True)
    
    async def get_groups_by_tag(self, tag, sys_user_id) -> list:
        res = await aio_mysql.execute_sql(f"""SELECT * FROM tg_group_configurations WHERE (JSON_CONTAINS(group_tag, JSON_ARRAY(CAST('{tag}' AS CHAR)), '$') AND created_by={sys_user_id} AND deleted_at IS NULL AND group_status=1) """)
        app_logger.info(f"通过标签获取群: sys_user_id: {sys_user_id}, tag: {tag}, res_sql: {res.msg}")
        if res.data:
            return res.data
        return []
    
    async def get_user_participation(self, user_id: str, activity_id: str) -> Dict:
        key = f"{user_id}_{activity_id}"
        return self.user_participations.get(key, {})
    
    async def save_user_participation(self, user_id: str, activity_id: str, data: Dict) -> None:
        key = f"{user_id}_{activity_id}"
        self.user_participations[key] = data
        
    async def update_activity_checked(self, activity_id: str, checked: int) -> None:
        await aio_mysql.execute_sql(f"UPDATE activity_list SET checked = {checked} WHERE id={activity_id}")
        
    async def get_winning_user(self, activity_id: str) -> list:
        res_user = await aio_mysql.execute_sql(f"SELECT * FROM activity_user WHERE activity_id = {activity_id} AND winning_status=1 ORDER BY prize_level")
        app_logger.info(f"获取某活动所有中奖用户: activity_id: {activity_id}, res_msg: {res_user.msg}, res_data: {res_user.data}")
        if res_user.data:
            return res_user.data
        return []
    
    async def get_finish_conditions_user(self, activity_id: str, tg_user_id) -> list:
        res_user = await aio_mysql.execute_sql(f"SELECT * FROM activity_user WHERE activity_id = {activity_id} AND condition_status=1 AND user_id={tg_user_id}")
        app_logger.info(f"获取某活动所有通过验证条件的用户: activity_id: {activity_id}, res_msg: {res_user.msg}, res_data: {res_user.data}")
        if res_user.data:
            return res_user.data
        return []
    
    async def get_close_activity_by_id(self, activity_id: str) -> Optional[Activity]:
        res = await self.get_all_activities(activity_id)
        return res
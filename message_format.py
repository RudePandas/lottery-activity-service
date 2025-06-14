
from abc import ABC, abstractmethod

from helper import *
from app.lottery_activity_handler.data_class import Activity
from app.lottery_activity_handler.data_repository import IDataRepository
from app.lottery_activity_handler.logger_handler import app_logger


class IMessageData(ABC):
    """消息格式化"""
    
    @abstractmethod
    async def reply_message_format(self) -> dict:
        pass
    
    @abstractmethod
    async def content_format(self) -> str:
        pass
    
    @abstractmethod
    async def start_notification(self) -> str:
        pass
    
    @abstractmethod
    async def end_notification(self) -> str:
        pass
    
    @abstractmethod
    async def start_command(self) -> str:
        pass
    
    @abstractmethod
    async def condition_check_not_finish(self) -> str:
        pass
    
    @abstractmethod
    async def condition_check_finish(self) -> str:
        pass
    
    @abstractmethod
    async def activity_close(self) -> str:
        pass
    
    
class InMessageFormat(IMessageData):
    """消息格式化实现"""
    
    async def __init__(self, activity: Activity, repository: IDataRepository):
        self.activity = activity
        self.repository = repository
        self.reply = await self.reply_message_format()
        self.numbers = [
                        "1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟",
                        "1️⃣1️⃣", "1️⃣2️⃣", "1️⃣3️⃣", "1️⃣4️⃣", "1️⃣5️⃣", "1️⃣6️⃣", "1️⃣7️⃣", "1️⃣8️⃣", "1️⃣9️⃣", "2️⃣0️⃣",
                        "2️⃣1️⃣", "2️⃣2️⃣", "2️⃣3️⃣", "2️⃣4️⃣", "2️⃣5️⃣", "2️⃣6️⃣", "2️⃣7️⃣", "2️⃣8️⃣", "2️⃣9️⃣", "3️⃣0️⃣",
                        "3️⃣1️⃣", "3️⃣2️⃣", "3️⃣3️⃣", "3️⃣4️⃣", "3️⃣5️⃣", "3️⃣6️⃣", "3️⃣7️⃣", "3️⃣8️⃣", "3️⃣9️⃣", "4️⃣0️⃣",
                        "4️⃣1️⃣", "4️⃣2️⃣", "4️⃣3️⃣", "4️⃣4️⃣", "4️⃣5️⃣", "4️⃣6️⃣", "4️⃣7️⃣", "4️⃣8️⃣", "4️⃣9️⃣", "5️⃣0️⃣",
                        "5️⃣1️⃣", "5️⃣2️⃣", "5️⃣3️⃣", "5️⃣4️⃣", "5️⃣5️⃣", "5️⃣6️⃣", "5️⃣7️⃣", "5️⃣8️⃣", "5️⃣9️⃣", "6️⃣0️⃣",
                        "6️⃣1️⃣", "6️⃣2️⃣", "6️⃣3️⃣", "6️⃣4️⃣", "6️⃣5️⃣", "6️⃣6️⃣", "6️⃣7️⃣", "6️⃣8️⃣", "6️⃣9️⃣", "7️⃣0️⃣",
                        "7️⃣1️⃣", "7️⃣2️⃣", "7️⃣3️⃣", "7️⃣4️⃣", "7️⃣5️⃣", "7️⃣6️⃣", "7️⃣7️⃣", "7️⃣8️⃣", "7️⃣9️⃣", "8️⃣0️⃣",
                        "8️⃣1️⃣", "8️⃣2️⃣", "8️⃣3️⃣", "8️⃣4️⃣", "8️⃣5️⃣", "8️⃣6️⃣", "8️⃣7️⃣", "8️⃣8️⃣", "8️⃣9️⃣", "9️⃣0️⃣",
                        "9️⃣1️⃣", "9️⃣2️⃣", "9️⃣3️⃣", "9️⃣4️⃣", "9️⃣5️⃣", "9️⃣6️⃣", "9️⃣7️⃣", "9️⃣8️⃣", "9️⃣9️⃣", "1️⃣0️⃣0️⃣"
                    ]

        
    async def reply_message_format(self) -> dict:
        # 中奖内容
        prize_content = ""
        for p in self.activity.prices:
            prize_content += "🔹 " + str(p.prize_name) + " " + str(p.prize_content) + " " + str(p.prize_count) + "人\n"
        # 中将条件
        conditions_content = ""
        group_names = []
        for condition in self.activity.conditions:
            if condition.type.value != "speech_count":
                conditions_content += condition.button_name + ": " + condition.target_id_link + "\n"
            else:
                groups = condition.target_id.split(',')
                for group in groups:
                    group_names.append(get_group(group)["group_name"])
                conditions_content += condition.button_name + ": " + condition.name + "\n" + "请在以下群组发言: " + '|'.join(group_names) + "\n"
        
        # 中奖名单
        winning_content = "\n"
        winning_user = await self.repository.get_winning_user(self.activity.id)
        for index, user in enumerate(winning_user):
            level = user['winning_content'].split(" ")[0]
            p = user['winning_content'].split(" ")[1]
            if user["user_name"] == 'None' or user["user_name"] is None:
                user_name = user["full_name"]
            else:
                user_name = f"@{user['user_name']}"
            winning_content += '⭐️' + " " + user_name + " " + level + " " + p + "\n"
                
        return {"prize_content": prize_content, 
                "conditions_content": conditions_content, 
                "winning_content": winning_content,
                "name": self.activity.name,
                "end_time": self.activity.end_time}
    
    async def content_format(self, content) -> str:
        return content.format(PRIZE_DRAW_NAME=self.reply["name"], 
                            PRIZE_CONTENT=self.reply["prize_content"], 
                            WINNING_TIME=self.reply["end_time"], 
                            WINNING_CONDITIONS=self.reply["conditions_content"], 
                            WINNING_LIST=self.reply["winning_content"])
    
    async def start_notification(self) -> str:
        content = self.activity.activities_reply[1].content
        content = await self.content_format(content)
        return content
    
    async def end_notification(self) -> str:
        content = self.activity.activities_reply[2].content
        content = await self.content_format(content)
        return content
    
    async def start_command(self) -> str:
        content = self.activity.activities_reply[3].content
        content = await self.content_format(content)
        return content
    
    async def condition_check_not_finish(self) -> str:
        content = self.activity.activities_reply[4].content
        content = await self.content_format(content)
        return content
    
    async def condition_check_finish(self) -> str:
        content = self.activity.activities_reply[5].content
        content = await self.content_format(content)
        return content
    
    async def activity_close(self) -> str:
        content = self.activity.activities_reply[7].content
        content = await self.content_format(content)
        return content
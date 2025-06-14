import json
from typing import Dict, List
from utils.common import bot_send_message, parser_text

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from mysql.aio import aio_mysql
from helper import *
from app.lottery_activity_handler.activity_scheduler import *
from app.lottery_activity_handler.data_repository import *
from app.lottery_activity_handler.validator import *
from app.lottery_activity_handler.logger_handler import app_logger


class LotteryService:
    """抽奖服务主类"""
    
    def __init__(self, repository: IDataRepository, bot_, message):
        self.repository = repository
        self.bot, self.sys_user_id, self.first_name, self.language = bot_
        self.message = message
    
    async def get_active_activities(self) -> List[Activity]:
        """获取激活的活动列表"""
        activities = await self.repository.get_all_activities()
        return [a for a in activities if a.is_active() and a.sys_user_id == self.sys_user_id]
    
    async def handle_activity_start(self, message_data: Dict) -> None:
        """处理活动开始消息"""
        app_logger.info(f"处理活动开始消息: {message_data}")
    
    async def handle_activity_end(self, message_data: Dict) -> None:
        """处理活动结束消息"""
        app_logger.info(f"处理活动结束消息: {message_data}")
    
    async def handle_user_action(self, message_data: Dict) -> None:
        """处理用户行为消息"""
        app_logger.info(f"处理用户行为消息: {message_data}")


class TelegramBotHandler:
    """Telegram机器人处理器"""
    
    def __init__(self, lottery_service: LotteryService, validator: ConditionValidatorFactory):
        self.lottery_service = lottery_service
        self.validator = validator
        
    async def _create_message_data(self, data):
        """生成发送的消息"""
        keyboard = []
        for row in data.buttons:
            current_button = []
            for button in row:
                inlineb_button = InlineKeyboardButton(button['text'], url=button['url'])
                current_button.append(inlineb_button)
            keyboard.append(current_button)
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        content = parser_text(data.content)
        pic_path = data.media if data.media else ""
        return {"content": content, "pic_path": pic_path, "reply_markup": reply_markup}
    
    async def start_command(self) -> None:
        """处理/start命令"""
        chat_id = self.lottery_service.message['from']['id']
        activities = await self.lottery_service.get_active_activities()
        
        if not activities: # 无活动
            activities_reply = await self.lottery_service.repository.get_all_reply(self.lottery_service.sys_user_id)
            reply_data = await self._create_message_data(activities_reply[6])
            res = await bot_send_message(self.lottery_service.bot, {}, reply_data["pic_path"], reply_data["content"], reply_data["reply_markup"], chat_id)
            app_logger.info(f"开始命令触发，无活动, 用户：{self.lottery_service.sys_user_id}")
            return
        
        keyboard = []
        for activity in activities:
            keyboard.append([
                InlineKeyboardButton(
                    f"🎯 {activity.name}",
                    callback_data=f"lottery_activity_{activity.id}"
                )
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        content = "以下为进行中的活动！"
        res = await bot_send_message(self.lottery_service.bot, {}, "", parser_text(content), reply_markup, chat_id)
        
    async def callback_query_handler(self) -> None:
        """处理回调查询"""
        data = self.lottery_service.message["data"]
        if data.startswith("lottery_activity_"):
            activity_id = data.replace("lottery_activity_", "")
            app_logger.info(f"选择活动回调, callback key: {data}, activyty_id: {activity_id}, callback_data: {self.lottery_service.message}")
            await self._handle_activity_selection(activity_id)
        
        elif data.startswith("lottery_condition_"):
            activity_id = data.replace("lottery_condition_", "")
            app_logger.info(f"选择发言条件回调, callback key: {data}, activyty_id: {activity_id}, callback_data: {self.lottery_service.message}")
            await self._handle_condition_selection(activity_id)
        
        elif data.startswith("lottery_check_"):
            activity_id = data.replace("lottery_check_", "")
            app_logger.info(f"选择检查活动个情况回调, callback key: {data}, activyty_id: {activity_id}, callback_data: {self.lottery_service.message}")
            await self._handle_condition_check(activity_id)
    
    async def _handle_activity_selection(self, activity_id: str) -> None:
        """处理活动选择"""
        try:
            chat_id = self.lottery_service.message["from"]["id"]
            activity = await self.lottery_service.repository.get_activity_by_id(activity_id)
            user_id = self.lottery_service.message.get("from", {}).get("id")
            app_logger.info(f"选择活动回调执行, activity: {activity}")
            # 活动结束或终止
            if not activity:
                activity_reply = await self.lottery_service.repository.get_all_reply(self.lottery_service.sys_user_id)
                reply_data = await self._create_message_data(activity_reply[7])
                close_activity = await self.lottery_service.repository.get_close_activity_by_id(activity_id)
                mesage_format = InMessageFormat(close_activity[0], self.lottery_service.repository)
                content = await mesage_format.activity_close()
                res = await bot_send_message(self.lottery_service.bot, {}, reply_data["pic_path"], content, reply_data["reply_markup"], chat_id)
                return
            
            # 先验证一遍再查用户条件，防止前面合格后面又不合格
            result = await self.validator.validate_user_conditions(self.lottery_service.repository, user_id, activity_id, self.lottery_service.bot, self.lottery_service.sys_user_id)
            user = await self.lottery_service.repository.get_finish_conditions_user(activity_id, user_id)
            # 参与活动完成所有条件了
            if user:
                reply_data = await self._create_message_data(activity.activities_reply[5])
                mesage_format = InMessageFormat(activity, self.lottery_service.repository)
                reply = mesage_format.reply_message_format()
                content = await mesage_format.condition_check_finish()
                res = await bot_send_message(self.lottery_service.bot, {}, reply_data["pic_path"], content, reply_data["reply_markup"], chat_id)
                return
            
            keyboard = []
            for condition in activity.conditions:
                if condition.type.value != "speech_count":
                    keyboard.append([
                        InlineKeyboardButton(
                            f"📋 {condition.button_name}",
                            url=condition.target_id_link
                        )
                    ])
                else:
                    keyboard.append([
                        InlineKeyboardButton(
                            f"📋 {condition.button_name}",
                            callback_data=f"lottery_condition_{activity.id}"
                        )
                    ])
            
            keyboard.append([
                InlineKeyboardButton(
                    "✅ 检查完成情况",
                    callback_data=f"lottery_check_{activity_id}"
                )
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            mesage_format = InMessageFormat(activity, self.lottery_service.repository)
            content = await mesage_format.start_command()
            pic_path = activity.activities_reply[3].media if activity.activities_reply[3].media else ""
            
            res = await bot_send_message(self.lottery_service.bot, {}, pic_path, parser_text(content), reply_markup, chat_id)
            
            res_sql = await self.lottery_service.repository.save_activity_detail(activity.id, self.lottery_service.message)
            app_logger.info(f"发送选择活动回调信息及保存用户参与活动记录: res_sql: {res_sql.msg}")
        except Exception as e:
            app_logger.error(f"选择活动回调执行异常: {e}", exc_info=True)
    
    async def _handle_condition_selection(self, activity_id: str) -> None:
        """处理条件选择(发言次数检查)"""
        try:
            activity = await self.lottery_service.repository.get_activity_by_id(activity_id)
            for condition in activity.conditions:
                if condition.type.value == "speech_count":
                    groups = condition.target_id.split(',')
                    text = ""
                    user_id = self.lottery_service.message["from"]["id"]
                    for group in groups:
                        activities_reply_data = await aio_mysql.execute_sql(f"SELECT * FROM chat_messages_logs WHERE user_id={user_id} AND chat_id={group} AND created_at>'{activity.start_time}' AND created_at<'{activity.end_time}'")
                        if not activities_reply_data.code:
                            app_logger.info(f"选择条件选择回调执行sql异常: sql: {activities_reply_data.msg}")
                        activities_reply_data = activities_reply_data.data
                        if activities_reply_data:
                            text += f"群发言次数：{activities_reply_data[0]['chat_title']}, 当前次数：{len(activities_reply_data)}, 达标次数：{condition.target_id_link}\n"
                        else:
                            text += f"群发言次数：{get_group(group)['group_name']}, 当前次数：{0}, 达标次数：{condition.target_id_link}\n"
            callback_query_id = self.lottery_service.message["id"]
            await self.lottery_service.bot.answer_callback_query(
                        callback_query_id=callback_query_id,
                        text=text,
                        show_alert=True
                    )
        except Exception as e:
            app_logger.error(f"选择条件选择回调执行异常: {e}", exc_info=True)
    
    async def _handle_condition_check(self, activity_id: str) -> None:
        """处理条件检查"""
        try:
            user_id = self.lottery_service.message["from"]["id"]
            result = await self.validator.validate_user_conditions(self.lottery_service.repository, user_id, activity_id, self.lottery_service.bot, self.lottery_service.sys_user_id)
            app_logger.info(f"处理验证检查情况 activity_id: {activity_id}, tg_user_id: {user_id}, result: {result}")
            if result.get("error"):
                return
            else:
                activity = await self.lottery_service.repository.get_activity_by_id(activity_id)
                app_logger.info(f"处理验证检查情况 活动实例 activity: {activity}")
                result = result.get("conditions")
                keyboard = []
                group_names = []
                for condition in activity.conditions:
                    if result[condition.type.value]["verified"]:
                        continue
                    if condition.type.value != "speech_count":
                        keyboard.append([
                            InlineKeyboardButton(
                                f"📋 {condition.button_name}",
                                url=condition.target_id_link
                            )
                        ])
                    else:
                        groups = condition.target_id.split(',')
                        for group in groups:
                            group_names.append(get_group(group)["group_name"])
                        keyboard.append([
                            InlineKeyboardButton(
                                f"📋 {condition.button_name}",
                                callback_data=f"lottery_condition_{activity.id}"
                            )
                        ])
                if keyboard: # 有未完成的用未完成的按钮
                    keyboard.append([
                        InlineKeyboardButton(
                            "✅ 检查完成情况",
                            callback_data=f"lottery_check_{activity_id}"
                        )
                    ])
                
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    mesage_format = InMessageFormat(activity, self.lottery_service.repository)
                    content = await mesage_format.condition_check_not_finish()
                    pic_path = activity.activities_reply[4].media if activity.activities_reply[4].media else ""
                    app_logger.info(f"处理验证检查情况, 有未完成的条件 activity_id: {activity_id}, keyboard: {keyboard}")
                else: # 完成的就用完成的按钮
                    mesage_format = InMessageFormat(activity, self.lottery_service.repository)
                    content = await mesage_format.condition_check_finish()
                    pic_path = activity.activities_reply[5].media if activity.activities_reply[5].media else ""
                    buttons = json.loads(activity.activities_reply[5].buttons) if activity.activities_reply[5].buttons else ""
                    keyboard = []
                    for row in buttons:
                        current_button = []
                        for button in row:
                            if "url" in button:
                                inlineb_button = InlineKeyboardButton(button['text'], url=button['url'])
                            else:
                                callback_data = '&'.join(str(item) for item in button['callback_data'])
                                inlineb_button = InlineKeyboardButton(button['text'], callback_data=callback_data)
                            current_button.append(inlineb_button)
                        keyboard.append(current_button)
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    # 更新状态
                    res_sql = await self.lottery_service.repository.update_activity_detail(activity.id, user_id, 1)
                    app_logger.info(f"处理验证检查情况, 全部条件完成 更新状态 activity_id: {activity_id}")
            message_id=self.lottery_service.message["message"]["message_id"]
            
            if pic_path:
                await self.lottery_service.bot.edit_message_caption(chat_id=user_id, text=content, message_id=message_id, parse_mode="HTML", reply_markup=reply_markup)
            else:
                await self.lottery_service.bot.edit_message_text(chat_id=user_id, text=content, message_id=message_id, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as e:
            app_logger.error(f"处理检查验证情况异常： {e}", exc_info=True)

class LotterySystem:
    """抽奖系统主类"""
    
    def __init__(self, bot_, message):
        self.bot, self.created_by, self.first_name, self.language = bot_
        self.repository = InMemoryRepository()
        self.lottery_service = LotteryService(self.repository, bot_, message)
        self.validator = ConditionValidatorFactory()
        self.bot_handler = TelegramBotHandler(self.lottery_service, self.validator)



async def callback_query_func(bot_, message):
    """按钮回调处理"""
    lottery_sys = LotterySystem(bot_, message)
    await lottery_sys.bot_handler.callback_query_handler()
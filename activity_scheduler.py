import asyncio
from abc import ABC, abstractmethod
import random
from datetime import datetime
from telegram import Bot

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from helper import *
from app.lottery_activity_handler.data_class import Activity, ActivityStatus
from app.lottery_activity_handler.data_repository import IDataRepository, InMemoryRepository
from app.lottery_activity_handler.message_format import InMessageFormat
from app.lottery_activity_handler.validator import *
from app.lottery_activity_handler.data_class import LotteryBot
from app.lottery_activity_handler.logger_handler import app_logger


class IPrizesChoice(ABC):
    """随机抽取中奖用户"""
    @abstractmethod
    async def random_choice_prizer(self, activity: Activity, chat_id: str) -> None:
        pass
    
class ActivityPrizesChoice(IPrizesChoice):
    async def random_choice_prizer(self, data_repository: IDataRepository, prezes_list: list, activity_detail_list: list) -> None:
        activity_detail_list_copy = activity_detail_list.copy()
    
        for index, prize in enumerate(prezes_list, start=1):
            if not activity_detail_list_copy:
                break
                
            # 随机取出元素
            taken = random.sample(activity_detail_list_copy, min(prize.prize_count, len(activity_detail_list_copy)))
            
            # 从列表中移除已取的元素
            for item in taken:
                activity_detail_list_copy.remove(item)
                
            for user in taken:
                await data_repository.update_prize_user(prize.prize_name + " " + prize.prize_content, user.id, index)
            app_logger.info(f"取出 {len(taken)} 个元素: {taken}, 剩余: {len(activity_detail_list_copy)} 个")
            
            if not activity_detail_list_copy:
                app_logger.info("列表2已清空")
                break

class INotificationService(ABC):
    """通知服务接口"""
    
    @abstractmethod
    async def send_activity_start_notification(self, activity: Activity, chat_id: str) -> None:
        pass
    
    @abstractmethod
    async def send_activity_end_notification(self, activity: Activity, chat_id: str) -> None:
        pass


class TelegramNotificationService(INotificationService):
    """Telegram通知服务"""
    async def send_activity_start_notification(self, activity: Activity, chat_id: str) -> None:
        """发送活动开始通知"""
        try:
            
            bot = await LotteryBot.get_first_bot(chat_id, "join_group", activity.sys_user_id)
            app_logger.info(f"发送活动开始通知 参数: activity: {activity}, chat_id: {chat_id}, first_bot: {bot}")
            repository = InMemoryRepository()
            mesage_format = InMessageFormat(activity, repository)
            content = await mesage_format.start_notification()
            
            lottery_bot = await LotteryBot.get_lottery_bot(activity.sys_user_id)
            bot_username = lottery_bot["username"].replace("@", "")
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 参与抽奖", url=f"https://t.me/{bot_username}")]
            ])
            
            await bot.send_message(
                chat_id=chat_id,
                text=content,
                reply_markup=keyboard
            )
        except Exception as e:
            app_logger.error(f"发送活动开始通知异常: {e}", exc_info=True)
        
    
    async def send_activity_end_notification(self, activity: Activity, chat_id: str, repository: IDataRepository) -> None:
        """发送活动结束通知"""
        try:
            repository = InMemoryRepository()
            mesage_format = InMessageFormat(activity, repository)
            content = await mesage_format.end_notification()
            
            bot = await LotteryBot.get_first_bot(chat_id, "join_group", activity.sys_user_id)
            await bot.send_message(chat_id=chat_id, text=content)
        except Exception as e:
            app_logger.error(f"发送活动结束通知异常: {e}", exc_info=True)

class ISchedulerService(ABC):
    """调度服务接口"""
    
    @abstractmethod
    async def task_scheduler(self) -> None:
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        pass


class ActivityScheduler(ISchedulerService):
    """活动调度器"""
    
    def __init__(self, repository: IDataRepository, notification_service: INotificationService, prizes_choice: ActivityPrizesChoice, validator: ConditionValidatorFactory):
        self.repository = repository
        self.notification_service = notification_service
        self.prizes_choice = prizes_choice
        self.validator = validator
        self._running = False
        self._task = None
        
    
    async def task_scheduler(self) -> None:
        """启动调度器"""
        self._running = True
        activity_scheduler = AsyncIOScheduler()
        activity_scheduler.add_job(self._scheduler_loop, 
                                'interval', 
                                seconds=60, 
                                misfire_grace_time=300,
                                max_instances=1, 
                                next_run_time=datetime.now())
        app_logger.info("活动调度器已启动")
        activity_scheduler.start()
    
    async def stop(self) -> None:
        """停止调度器"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        app_logger.info("活动调度器已停止")
    
    # async def _scheduler_loop(self) -> None:
    #     """调度循环"""
    #     activities = await self.repository.get_all_activities()
    #     for activity in activities:
    #         try:
    #             if activity.should_start():
    #                 activity.status = ActivityStatus.ACTIVE.value
    #                 await self.repository.set_activity_status(activity.id, ActivityStatus.ACTIVE.value)
    #                 if activity.scope.startswith("-100") and activity.checked==0:
    #                     await self.notification_service.send_activity_start_notification(
    #                         activity, activity.scope
    #                     )
    #                 elif not activity.scope.startswith("-100") and activity.checked==0:
    #                     groups = await self.repository.get_groups_by_tag(activity.scope, activity.sys_user_id)
    #                     for group in groups:
    #                         await self.notification_service.send_activity_start_notification(
    #                             activity, group["group_id"]
    #                         )
    #                 app_logger.info(f"活动已开始 activity: {activity} 范围scope： {activity.scope}")
    #                 await self.repository.update_activity_checked(activity.id, 1)
    #             elif activity.should_end():
    #                 # if int((activity.end_time - activity.start_time).seconds) < 1800: # 活动时间小于半小时直接结束验证，不做结束半小时前检查
    #                 # 检查所有用户验证条件
    #                 lottery_bot = await LotteryBot.get_lottery_bot(activity.sys_user_id)
    #                 bot = Bot(lottery_bot["token"])
    #                 for user in activity.activity_users:
    #                     result = await self.validator.validate_user_conditions(self.repository, user.user_id, activity.id, bot, activity.sys_user_id)
                        
    #                 finish_condition_users = [user for user in activity.activity_users if user.condition_status]
    #                 await self.prizes_choice.random_choice_prizer(self.repository, activity.prices, finish_condition_users)
    #                 activity.status = ActivityStatus.ENDED.value
    #                 await self.repository.set_activity_status(activity.id, ActivityStatus.ENDED.value)
    #                 if activity.scope.startswith("-100") and activity.checked!=0:
    #                     await self.notification_service.send_activity_end_notification(
    #                         activity, activity.scope, self.repository
    #                     )
    #                 elif not activity.scope.startswith("-100") and activity.checked!=0:
    #                     groups = await self.repository.get_groups_by_tag(activity.scope, activity.sys_user_id)
    #                     for group in groups:
    #                         await self.notification_service.send_activity_end_notification(
    #                             activity, group["group_id"], self.repository
    #                         )
    #                 app_logger.info(f"活动已结束 {activity}  范围scope： {activity.scope}")
    #             elif activity.should_check():
    #                 lottery_bot = await LotteryBot.get_lottery_bot(activity.sys_user_id)
    #                 bot = Bot(lottery_bot["token"])
    #                 for user in activity.activity_users:
    #                     result = await self.validator.validate_user_conditions(self.repository, user.user_id, activity.id, bot, activity.sys_user_id)
    #         except Exception as e:
    #             app_logger.error(f"调度器执行出错: {e}", exc_info=True)
                
    async def _scheduler_loop(self) -> None:
        """调度循环"""
        try:
            activities = await self.repository.get_all_activities()
            
            # 并发处理活动，但限制并发数量避免资源耗尽
            semaphore = asyncio.Semaphore(10)  # 最多同时处理10个活动
            tasks = [self._process_activity(activity, semaphore) for activity in activities]
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            app_logger.error(f"调度器循环执行出错: {e}", exc_info=True)

    async def _process_activity(self, activity, semaphore: asyncio.Semaphore) -> None:
        """处理单个活动"""
        async with semaphore:
            try:
                if activity.should_start():
                    await self._handle_activity_start(activity)
                elif activity.should_end():
                    await self._handle_activity_end(activity)
                elif activity.should_check():
                    await self._handle_activity_check(activity)
            except Exception as e:
                app_logger.error(f"处理活动 {activity.id} 时出错: {e}", exc_info=True)

    async def _handle_activity_start(self, activity) -> None:
        """处理活动开始"""
        if activity.checked != 0:  # 已经处理过开始通知
            return
            
        # 更新活动状态
        activity.status = ActivityStatus.ACTIVE.value
        await self.repository.set_activity_status(activity.id, ActivityStatus.ACTIVE.value)
        
        # 发送开始通知
        await self._send_activity_notification(
            activity, 
            self.notification_service.send_activity_start_notification
        )
        
        # 标记为已检查
        await self.repository.update_activity_checked(activity.id, 1)
        app_logger.info(f"活动已开始 activity: {activity} 范围scope: {activity.scope}")

    async def _handle_activity_end(self, activity) -> None:
        """处理活动结束"""
        if activity.checked == 0:  # 还未开始就不能结束
            return
            
        # 验证用户条件并选择获奖者
        await self._validate_and_choose_winners(activity)
        
        # 更新活动状态
        activity.status = ActivityStatus.ENDED.value
        await self.repository.set_activity_status(activity.id, ActivityStatus.ENDED.value)
        
        # 发送结束通知
        await self._send_activity_notification(
            activity, 
            lambda act, scope: self.notification_service.send_activity_end_notification(
                act, scope, self.repository
            )
        )
        
        app_logger.info(f"活动已结束 {activity} 范围scope: {activity.scope}")

    async def _handle_activity_check(self, activity) -> None:
        """处理活动检查"""
        try:
            bot = await self._get_bot(activity.sys_user_id)
            await self._validate_users_conditions(activity, bot)
        except Exception as e:
            app_logger.error(f"活动检查失败 {activity.id}: {e}", exc_info=True)

    async def _send_activity_notification(self, activity, notification_func) -> None:
        """发送活动通知（统一处理单个群组和标签群组）"""
        try:
            if activity.scope.startswith("-100"):
                # 单个群组
                await notification_func(activity, activity.scope)
            else:
                # 标签群组
                groups = await self.repository.get_groups_by_tag(
                    activity.scope, 
                    activity.sys_user_id
                )
                
                # 并发发送通知，但限制并发数
                semaphore = asyncio.Semaphore(5)  # 最多同时发送5个通知
                tasks = [
                    self._send_single_notification(notification_func, activity, group, semaphore)
                    for group in groups
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            app_logger.error(f"发送活动通知失败 {activity.id}: {e}", exc_info=True)

    async def _send_single_notification(self, notification_func, activity, group, semaphore):
        """发送单个通知"""
        async with semaphore:
            try:
                await notification_func(activity, group["group_id"])
            except Exception as e:
                app_logger.error(f"发送通知到群组 {group['group_id']} 失败: {e}")

    async def _validate_and_choose_winners(self, activity) -> None:
        """验证用户条件并选择获奖者"""
        try:
            bot = await self._get_bot(activity.sys_user_id)
            await self._validate_users_conditions(activity, bot)
            
            # 用activity_id重新获取下活动
            activity = await self.repository.get_all_activities(activity_id=activity.id)
            activity = activity[0]
            
            # 选择获奖者
            finish_condition_users = [
                user for user in activity.activity_users 
                if user.condition_status
            ]
            
            if finish_condition_users and activity.prices:
                await self.prizes_choice.random_choice_prizer(
                    self.repository, 
                    activity.prices, 
                    finish_condition_users
                )
            else:
                app_logger.info(f"活动 {activity.id} 无符合条件的用户或无奖品")
                
        except Exception as e:
            app_logger.error(f"验证用户条件和选择获奖者失败 {activity.id}: {e}", exc_info=True)

    async def _validate_users_conditions(self, activity, bot) -> None:
        """验证所有用户条件"""
        if not activity.activity_users:
            return
            
        # 并发验证用户条件，但限制并发数
        semaphore = asyncio.Semaphore(20)  # 最多同时验证20个用户
        tasks = [
            self._validate_single_user(user, activity, bot, semaphore)
            for user in activity.activity_users
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 记录验证失败的用户
        # failed_count = sum(1 for result in results if isinstance(result, Exception))
        # if failed_count > 0:
        #     app_logger.warning(f"活动 {activity.id} 有 {failed_count} 个用户验证失败")

    async def _validate_single_user(self, user, activity, bot, semaphore):
        """验证单个用户条件"""
        async with semaphore:
            try:
                return await self.validator.validate_user_conditions(
                    self.repository, 
                    user.user_id, 
                    activity.id, 
                    bot, 
                    activity.sys_user_id
                )
            except Exception as e:
                app_logger.error(f"验证用户 {user.user_id} 条件失败: {e}")
                raise

    async def _get_bot(self, sys_user_id: int) -> Bot:
        """获取机器人实例（可以添加缓存）"""
        try:
            lottery_bot = await LotteryBot.get_lottery_bot(sys_user_id)
            return Bot(lottery_bot["token"])
        except Exception as e:
            app_logger.error(f"获取机器人失败 sys_user_id: {sys_user_id}, error: {e}")
            raise

            
async def lottery_activity_scheduler():
    repository = InMemoryRepository()
    notification = TelegramNotificationService()
    prizes_choice = ActivityPrizesChoice()
    validator = ConditionValidatorFactory()
    LotteryBot()
    activity_scheduler = ActivityScheduler(repository, notification, prizes_choice, validator)
    return await activity_scheduler.task_scheduler()
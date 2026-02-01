import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)
from supabase import create_client, Client
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib import colors
from reportlab.lib.units import cm

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://YOUR_PROJECT.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "YOUR_SUPABASE_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_BOT_TOKEN")
ADMIN_IDS = json.loads(os.getenv("ADMIN_IDS", "[]"))  # Список ID администраторов

# Инициализация Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Команды бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    telegram_id = user.id
    
    # Регистрируем пользователя в базе
    try:
        user_data = {
            "telegram_id": telegram_id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "is_admin": telegram_id in ADMIN_IDS
        }
        
        # Проверяем, есть ли уже пользователь
        response = supabase.table("bot_users").select("*").eq("telegram_id", telegram_id).execute()
        
        if len(response.data) == 0:
            supabase.table("bot_users").insert(user_data).execute()
            logger.info(f"Зарегистрирован новый пользователь: {user.username}")
    
    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя: {e}")
    
    welcome_text = f"""
👋 Привет, {user.first_name}!

Я - бот для отслеживания логистических заказов компании Margiana Logistic Services.

📋 **Доступные команды:**
/orders - Список активных заказов
/completed - Завершенные заказы (последние 30 дней)
/status [статус] - Заказы по статусу
/missing_photos - Заказы без фото загрузки
/upcoming - Предстоящие события
/report - Получить отчет в PDF
/help - Помощь

🔔 Бот автоматически уведомляет о ключевых событиях:
• Создание новых заказов
• Изменение статусов
• Прибытие/отправление контейнеров
• Предстоящие сроки

Для администраторов доступны дополнительные команды:
/stats - Статистика
/notify - Отправить уведомление всем
"""
    
    await update.message.reply_text(welcome_text)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = """
📖 **Помощь по командам:**

**Основные команды:**
/orders - Показать активные заказы
/completed - Завершенные заказы за 30 дней
/status [статус] - Фильтр по статусу
    Доступные статусы: New, In Progress CHN, In Transit CHN-IR, 
    In Progress IR, In Transit IR-TKM, Completed, Cancelled
/missing_photos - Заказы без фото загрузки
/upcoming - События на ближайшие 7 дней
/report - Создать PDF отчет

**Примеры:**
/status "In Transit CHN-IR"
/status Completed

**Для администраторов:**
/stats - Статистика системы
/notify [текст] - Отправить уведомление всем пользователям
"""
    await update.message.reply_text(help_text)


async def show_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать активные заказы"""
    try:
        # Получаем события из облачной БД
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        
        response = supabase.table("cloud_sync_log")\
            .select("*")\
            .neq("event_type", "ORDER_DELETED")\
            .gte("created_at", thirty_days_ago)\
            .order("created_at", desc=True)\
            .execute()
        
        if not response.data:
            await update.message.reply_text("📭 Активных заказов не найдено")
            return
        
        # Группируем по order_id
        orders_dict = {}
        for event in response.data:
            order_id = event['order_id']
            if order_id not in orders_dict:
                orders_dict[order_id] = event
            elif event['created_at'] > orders_dict[order_id]['created_at']:
                orders_dict[order_id] = event
        
        # Формируем сообщение
        message_lines = ["📋 **Активные заказы:**\n"]
        
        for idx, (order_id, latest_event) in enumerate(orders_dict.items(), 1):
            event_data = latest_event.get('event_data', {})
            
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except:
                    event_data = {}
            
            order_info = f"""
{idx}. **Заказ #{latest_event.get('order_number', order_id)}**
   👤 Клиент: {event_data.get('client', 'Не указан')}
   📦 Контейнеров: {event_data.get('containers', 0)}
   ⚖️ Вес: {event_data.get('weight', 0)} кг
   📍 Статус: {event_data.get('status', 'Неизвестен')}
   🕐 Последнее обновление: {latest_event['created_at'][:10]}
"""
            message_lines.append(order_info)
        
        # Разбиваем сообщение если слишком длинное
        full_message = "\n".join(message_lines)
        if len(full_message) > 4000:
            parts = [full_message[i:i+4000] for i in range(0, len(full_message), 4000)]
            for part in parts:
                await update.message.reply_text(part, parse_mode='Markdown')
        else:
            await update.message.reply_text(full_message, parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Ошибка получения заказов: {e}")
        await update.message.reply_text("❌ Ошибка при получении данных")


async def show_completed_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать завершенные заказы за 30 дней"""
    try:
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        
        response = supabase.table("cloud_sync_log")\
            .select("*")\
            .eq("event_data->>status", "Completed")\
            .gte("created_at", thirty_days_ago)\
            .order("created_at", desc=True)\
            .execute()
        
        if not response.data:
            await update.message.reply_text("✅ Нет завершенных заказов за последние 30 дней")
            return
        
        # Группируем уникальные заказы
        completed_orders = {}
        for event in response.data:
            order_id = event['order_id']
            if order_id not in completed_orders:
                completed_orders[order_id] = event
        
        message_lines = ["✅ **Завершенные заказы (30 дней):**\n"]
        
        for idx, (order_id, event) in enumerate(completed_orders.items(), 1):
            event_data = event.get('event_data', {})
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except:
                    event_data = {}
            
            order_info = f"""
{idx}. **#{event.get('order_number', order_id)}** - {event_data.get('client', 'Клиент')}
   📅 Завершен: {event['created_at'][:10]}
   📦 Контейнеров: {event_data.get('containers', 0)}
   ⚖️ Вес: {event_data.get('weight', 0)} кг
"""
            message_lines.append(order_info)
        
        full_message = "\n".join(message_lines)
        await update.message.reply_text(full_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Ошибка при получении данных")


async def filter_by_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Фильтр заказов по статусу"""
    if not context.args:
        await update.message.reply_text(
            "ℹ️ Использование: /status [статус]\n"
            "Пример: /status \"In Transit CHN-IR\"\n\n"
            "Доступные статусы:\n"
            "• New\n• In Progress CHN\n• In Transit CHN-IR\n"
            "• In Progress IR\n• In Transit IR-TKM\n• Completed\n• Cancelled"
        )
        return
    
    status_query = " ".join(context.args)
    
    try:
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        
        # Ищем события с указанным статусом
        response = supabase.table("cloud_sync_log")\
            .select("*")\
            .like("event_data->>status", f"%{status_query}%")\
            .gte("created_at", thirty_days_ago)\
            .order("created_at", desc=True)\
            .execute()
        
        if not response.data:
            await update.message.reply_text(f"📭 Заказов со статусом '{status_query}' не найдено")
            return
        
        message_lines = [f"🔍 **Заказы со статусом '{status_query}':**\n"]
        
        for idx, event in enumerate(response.data[:20], 1):  # Ограничиваем 20 заказами
            event_data = event.get('event_data', {})
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except:
                    event_data = {}
            
            order_info = f"""
{idx}. **#{event.get('order_number', event['order_id'])}**
   👤 {event_data.get('client', 'Клиент')}
   📦 {event_data.get('containers', 0)} контейнер(ов)
   ⚖️ {event_data.get('weight', 0)} кг
   🕐 {event['created_at'][:10]}
"""
            message_lines.append(order_info)
        
        if len(response.data) > 20:
            message_lines.append(f"\n... и еще {len(response.data) - 20} заказов")
        
        full_message = "\n".join(message_lines)
        await update.message.reply_text(full_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка фильтрации: {e}")
        await update.message.reply_text("❌ Ошибка при фильтрации")


async def show_missing_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать заказы без фото загрузки"""
    try:
        # Получаем все события заказов
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        
        response = supabase.table("cloud_sync_log")\
            .select("*")\
            .gte("created_at", thirty_days_ago)\
            .order("created_at", desc=True)\
            .execute()
        
        if not response.data:
            await update.message.reply_text("📭 Данных не найдено")
            return
        
        # Ищем заказы с событием MISSING_PHOTO или без фото
        orders_without_photos = []
        
        for event in response.data:
            if event.get('event_type') == "MISSING_PHOTO":
                orders_without_photos.append(event)
        
        if not orders_without_photos:
            await update.message.reply_text("✅ Все заказы имеют фото загрузки!")
            return
        
        message_lines = ["📷 **Заказы без фото загрузки:**\n"]
        
        for idx, event in enumerate(orders_without_photos[:15], 1):
            event_data = event.get('event_data', {})
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except:
                    event_data = {}
            
            order_info = f"""
{idx}. **#{event.get('order_number', event['order_id'])}**
   👤 {event_data.get('client', 'Клиент')}
   📍 {event_data.get('status', 'Статус')}
   🕐 {event['created_at'][:10]}
"""
            message_lines.append(order_info)
        
        full_message = "\n".join(message_lines)
        await update.message.reply_text(full_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Ошибка при получении данных")


async def show_upcoming_events(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать предстоящие события"""
    try:
        today = datetime.now().date()
        next_week = today + timedelta(days=7)
        
        # Ищем события UPCOMING_DEADLINE
        response = supabase.table("cloud_sync_log")\
            .select("*")\
            .eq("event_type", "UPCOMING_DEADLINE")\
            .gte("created_at", today.isoformat())\
            .lte("created_at", next_week.isoformat())\
            .order("created_at")\
            .execute()
        
        if not response.data:
            await update.message.reply_text("📅 Нет предстоящих событий на ближайшую неделю")
            return
        
        message_lines = ["📅 **Предстоящие события (7 дней):**\n"]
        
        for event in response.data:
            event_data = event.get('event_data', {})
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except:
                    event_data = {}
            
            event_info = f"""
📌 **{event_data.get('title', 'Событие')}**
   Заказ: #{event.get('order_number', event['order_id'])}
   Дата: {event['created_at'][:10]}
   Описание: {event_data.get('description', 'Нет описания')}
"""
            message_lines.append(event_info)
        
        full_message = "\n".join(message_lines)
        await update.message.reply_text(full_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Ошибка при получении данных")


async def generate_pdf_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создать PDF отчет"""
    try:
        await update.message.reply_text("📊 Формирую отчет... Это займет несколько секунд.")
        
        # Создаем PDF файл
        filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        # Создаем документ
        doc = SimpleDocTemplate(filename, pagesize=A4)
        story = []
        
        # Стили
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#2C3E50'),
            spaceAfter=30
        )
        
        # Заголовок
        story.append(Paragraph("Margiana Logistic Services", title_style))
        story.append(Paragraph(f"Отчет от {datetime.now().strftime('%d.%m.%Y %H:%M')}", styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Получаем данные
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        
        response = supabase.table("cloud_sync_log")\
            .select("*")\
            .gte("created_at", thirty_days_ago)\
            .order("created_at", desc=True)\
            .execute()
        
        # Статистика
        total_events = len(response.data)
        completed_orders = sum(1 for e in response.data 
                              if isinstance(e.get('event_data'), str) and '"Completed"' in e.get('event_data', ''))
        
        # Добавляем статистику
        story.append(Paragraph("Общая статистика:", styles['Heading2']))
        stats_data = [
            ["Показатель", "Значение"],
            ["Всего событий (30 дней)", str(total_events)],
            ["Завершенных заказов", str(completed_orders)],
            ["Сгенерирован", datetime.now().strftime('%d.%m.%Y %H:%M')]
        ]
        
        stats_table = Table(stats_data, colWidths=[10*cm, 6*cm])
        stats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(stats_table)
        story.append(Spacer(1, 30))
        
        # Последние события
        story.append(Paragraph("Последние события:", styles['Heading2']))
        
        events_data = [["Дата", "Тип события", "Заказ", "Детали"]]
        
        for event in response.data[:10]:  # Последние 10 событий
            event_data = event.get('event_data', '{}')
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except:
                    event_data = {}
            
            events_data.append([
                event['created_at'][:10],
                event['event_type'],
                event.get('order_number', str(event['order_id'])),
                event_data.get('client', '')[0:20] + "..." if len(event_data.get('client', '')) > 20 else event_data.get('client', '')
            ])
        
        events_table = Table(events_data, colWidths=[3*cm, 5*cm, 3*cm, 5*cm])
        events_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495E')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
        ]))
        
        story.append(events_table)
        
        # Создаем PDF
        doc.build(story)
        
        # Отправляем файл пользователю
        with open(filename, 'rb') as file:
            await update.message.reply_document(
                document=file,
                caption="📄 Ваш отчет готов!",
                filename=f"Отчет_{datetime.now().strftime('%d.%m.%Y')}.pdf"
            )
        
        # Удаляем временный файл
        os.remove(filename)
        
    except Exception as e:
        logger.error(f"Ошибка создания PDF: {e}")
        await update.message.reply_text("❌ Ошибка при создании отчета")


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать статистику (только для администраторов)"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Эта команда только для администраторов")
        return
    
    try:
        # Статистика пользователей
        users_response = supabase.table("bot_users").select("count").execute()
        users_count = users_response.data[0]['count'] if users_response.data else 0
        
        # Статистика событий
        events_response = supabase.table("cloud_sync_log").select("count").execute()
        events_count = events_response.data[0]['count'] if events_response.data else 0
        
        # Статистика за последние 7 дней
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        weekly_response = supabase.table("cloud_sync_log")\
            .select("event_type")\
            .gte("created_at", week_ago)\
            .execute()
        
        weekly_stats = {}
        for event in weekly_response.data:
            event_type = event['event_type']
            weekly_stats[event_type] = weekly_stats.get(event_type, 0) + 1
        
        # Формируем сообщение
        stats_text = f"""
📊 **Статистика системы:**

👥 **Пользователи:**
• Всего пользователей: {users_count}
• Администраторов: {len(ADMIN_IDS)}

📈 **События:**
• Всего событий: {events_count}
• За последние 7 дней: {len(weekly_response.data)}

📅 **Активность за неделю:**
"""
        
        for event_type, count in sorted(weekly_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
            stats_text += f"• {event_type}: {count}\n"
        
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка статистики: {e}")
        await update.message.reply_text("❌ Ошибка при получении статистики")


async def send_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправить уведомление всем пользователям (администратор)"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Эта команда только для администраторов")
        return
    
    if not context.args:
        await update.message.reply_text("ℹ️ Использование: /notify [текст уведомления]")
        return
    
    notification_text = " ".join(context.args)
    
    try:
        # Получаем всех пользователей
        users_response = supabase.table("bot_users").select("telegram_id").execute()
        
        if not users_response.data:
            await update.message.reply_text("📭 Нет зарегистрированных пользователей")
            return
        
        sent_count = 0
        failed_count = 0
        
        for user in users_response.data:
            try:
                await context.bot.send_message(
                    chat_id=user['telegram_id'],
                    text=f"🔔 **Уведомление от администратора:**\n\n{notification_text}",
                    parse_mode='Markdown'
                )
                sent_count += 1
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление пользователю {user['telegram_id']}: {e}")
                failed_count += 1
        
        await update.message.reply_text(
            f"📨 Уведомление отправлено:\n"
            f"✅ Успешно: {sent_count}\n"
            f"❌ Не удалось: {failed_count}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка отправки уведомлений: {e}")
        await update.message.reply_text("❌ Ошибка при отправке уведомлений")


async def handle_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик неизвестных команд"""
    await update.message.reply_text(
        "❓ Неизвестная команда. Используйте /help для списка команд."
    )


async def check_notifications(context: ContextTypes.DEFAULT_TYPE):
    """Проверка и отправка отложенных уведомлений"""
    try:
        # Получаем ожидающие уведомления
        response = supabase.table("notifications_queue")\
            .select("*")\
            .eq("status", "pending")\
            .order("created_at")\
            .limit(10)\
            .execute()
        
        for notification in response.data:
            try:
                await context.bot.send_message(
                    chat_id=notification['telegram_id'],
                    text=notification['message_text'],
                    parse_mode='Markdown'
                )
                
                # Помечаем как отправленное
                supabase.table("notifications_queue")\
                    .update({"status": "sent", "sent_at": datetime.now().isoformat()})\
                    .eq("id", notification['id'])\
                    .execute()
                    
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления {notification['id']}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка проверки уведомлений: {e}")


def main():
    """Запуск бота"""
    # Создаем приложение
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("orders", show_orders))
    application.add_handler(CommandHandler("completed", show_completed_orders))
    application.add_handler(CommandHandler("status", filter_by_status))
    application.add_handler(CommandHandler("missing_photos", show_missing_photos))
    application.add_handler(CommandHandler("upcoming", show_upcoming_events))
    application.add_handler(CommandHandler("report", generate_pdf_report))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("notify", send_notification))
    
    # Обработчик неизвестных команд
    application.add_handler(MessageHandler(filters.COMMAND, handle_unknown))
    
    # Настраиваем job queue для проверки уведомлений
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(check_notifications, interval=30, first=10)
    
    # Запускаем бота
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
# type: ignore
"""
Fudly Telegram Bot - Main Module

This file is being refactored to use modular handlers from the handlers/ package.
See handlers/README.md for details on the refactoring structure.

Current status: Foundation laid with handlers/common.py, handlers/registration.py,
handlers/user_commands.py, and handlers/admin.py created. Full integration pending.
"""
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile
import asyncio
import os
import random
import string
import socket
import sys
import signal
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from database import Database
from keyboards import *
from keyboards import units_keyboard, product_categories_keyboard, offers_category_filter, stores_category_selection, booking_filters_keyboard
from localization import get_text, get_cities, get_categories, normalize_category

# In-memory per-session view mode override: {'seller'|'customer'}
user_view_mode = {}

# Production optimizations (optional imports with fallbacks)
try:
    from security import validator, rate_limiter, secure_user_input, validate_admin_action
    from logging_config import logger
    from background import start_background_tasks
    PRODUCTION_FEATURES = True
except ImportError as e:
    print(f"⚠️ Production features not available: {e}")
    # Create fallback implementations
    class FallbackValidator:
        @staticmethod
        def sanitize_text(text, max_length=1000):
            return str(text)[:max_length] if text else ""
        @staticmethod
        def validate_city(city):
            return bool(city and len(city) < 50)
    
    class FallbackRateLimiter:
        def is_allowed(self, *args, **kwargs):
            return True
    
    validator = FallbackValidator()
    rate_limiter = FallbackRateLimiter()
    
    def secure_user_input(func):
        return func
    
    def validate_admin_action(user_id, db):
        return db.is_admin(user_id)
    
    import logging
    logger = logging.getLogger('fudly')
    
    def start_background_tasks(db):
        print("Background tasks disabled (dependencies not available)")
    
    PRODUCTION_FEATURES = False

# Load environment variables
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # Для Railway: https://yourapp.railway.app
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
PORT = int(os.getenv("PORT", 8000))
USE_WEBHOOK = os.getenv("USE_WEBHOOK", "false").lower() == "true"
SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN", None)  # Опциональный токен для безопасности webhook
    # ...existing code...

# Simple in-process metrics (no external deps)
METRICS = {
    "updates_received": 0,
    "updates_errors": 0,
    "bookings_created": 0,
    "bookings_cancelled": 0,
}

# Basic rate-limit config
RL_MAX = int(os.getenv("MAX_REQUESTS_PER_MINUTE", 30))
RL_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", 60))

def can_proceed(user_id: int, action: str) -> bool:
    """Wrapper around rate_limiter if available. Fallback always allows."""
    try:
        return rate_limiter.is_allowed(user_id=user_id, action=action, max_requests=RL_MAX, window_seconds=RL_WINDOW)
    except Exception:
        try:
            return rate_limiter.is_allowed(user_id, action)
        except Exception:
            return True

# Словарь для преобразования узбекских названий городов в русские
CITY_UZ_TO_RU = {
    "Toshkent": "Ташкент",
    "Samarqand": "Самарканд",
    "Buxoro": "Бухара",
    "Andijon": "Андижан",
    "Namangan": "Наманган",
    "Farg'ona": "Фергана",
    "Xiva": "Хива",
    "Nukus": "Нукус"
}

def normalize_city(city: str) -> str:
    """Преобразует название города в русский формат для поиска в БД"""
    return CITY_UZ_TO_RU.get(city, city)

def normalize_category(category: str) -> str:
    """Преобразует отображаемое название категории в английский ключ для БД"""
    category_map = {
        '🍞 Хлеб и выпечка': 'bakery', '🥛 Молочные продукты': 'dairy', '🥩 Мясо и птица': 'meat',
        '🐟 Рыба и морепродукты': 'fish', '🥬 Овощи': 'vegetables', '🍎 Фрукты и ягоды': 'fruits',
        '🧀 Сыры': 'cheese', '🥚 Яйца': 'eggs', '🍚 Крупы и макароны': 'grains',
        '🥫 Консервы': 'canned', '🍫 Кондитерские изделия': 'sweets', '🍪 Печенье и снэки': 'snacks',
        '☕ Чай и кофе': 'drinks_hot', '🥤 Напитки': 'drinks', '🧴 Бытовая химия': 'household',
        '🧼 Гигиена': 'hygiene', '🏠 Для дома': 'home', '🎯 Другое': 'other',
        # Узбекские названия
        '🍞 Non va pishiriq': 'bakery', '🥛 Sut mahsulotlari': 'dairy', '🥩 Go\'sht va parranda': 'meat',
        '🐟 Baliq': 'fish', '🥬 Sabzavotlar': 'vegetables', '🍎 Mevalar': 'fruits',
        '🧀 Pishloqlar': 'cheese', '🥚 Tuxum': 'eggs', '🍚 Don mahsulotlari': 'grains',
        '🥫 Konserva': 'canned', '🍫 Shirinliklar': 'sweets', '🍪 Pechene va gazaklar': 'snacks',
        '☕ Choy va qahva': 'drinks_hot', '🥤 Ichimliklar': 'drinks', '🧴 Uy-joy uchun': 'household',
        '🧼 Gigiena': 'hygiene', '🏠 Uy uchun': 'home', '🎯 Boshqalar': 'other',
        # Также поддержка без эмодзи (если они были удалены)
        'Хлеб и выпечка': 'bakery', 'Молочные продукты': 'dairy', 'Мясо и птица': 'meat',
        'Рыба и морепродукты': 'fish', 'Овощи': 'vegetables', 'Фрукты и ягоды': 'fruits',
        'Сыры': 'cheese', 'Яйца': 'eggs', 'Крупы и макароны': 'grains',
        'Консервы': 'canned', 'Кондитерские изделия': 'sweets', 'Печенье и снэки': 'snacks',
        'Чай и кофе': 'drinks_hot', 'Напитки': 'drinks', 'Бытовая химия': 'household',
        'Гигиена': 'hygiene', 'Для дома': 'home', 'Другое': 'other'
    }
    # Убираем эмодзи, если название только из эмодзи (пришло из кнопки фильтра)
    cleaned = category.strip()
    # Если в тексте есть эмодзи в начале, возвращаем маппинг
    result = category_map.get(cleaned, cleaned.lower())
    return result if result in category_map.values() else 'other'

# Узбекская временная зона (UTC+5)
UZB_TZ = timezone(timedelta(hours=5))

def get_uzb_time():
    """Получить текущее время в узбекской временной зоне (UTC+5)"""
    return datetime.now(UZB_TZ)

def has_approved_store(user_id: int) -> bool:
    """Проверяет, есть ли у пользователя одобренный магазин"""
    stores = db.get_user_stores(user_id)
    # stores: [0]store_id, [1]owner_id, [2]name, [3]city, [4]address, [5]description, 
    #         [6]category, [7]phone, [8]status, [9]rejection_reason, [10]created_at
    return any(store[8] == "active" for store in stores if len(store) > 8)

def get_appropriate_menu(user_id: int, lang: str):
    """Возвращает подходящее меню для пользователя с проверкой статуса магазина"""
    user = db.get_user(user_id)
    if not user:
        return main_menu_customer(lang)
    
    role = user[6] if len(user) > 6 else "customer"
    
    # Если партнёр - проверяем наличие одобренного магазина
    if role == "seller":
        if has_approved_store(user_id):
            return main_menu_seller(lang)
        else:
            # Нет одобренного магазина - показываем меню покупателя
            return main_menu_customer(lang)
    
    return main_menu_customer(lang)

# Initialize bot, dispatcher and database
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = Database()

# Import state classes and utilities from handlers package
from handlers.common import (
    Registration, RegisterStore, CreateOffer, BulkCreate,
    ChangeCity, EditOffer, ConfirmOrder, BookOffer,
    RegistrationCheckMiddleware, user_view_mode as handler_user_view_mode,
    has_approved_store as handler_has_approved_store,
    get_appropriate_menu as handler_get_appropriate_menu
)

# Use imported utilities (override local definitions)
user_view_mode = handler_user_view_mode
has_approved_store = lambda user_id: handler_has_approved_store(user_id, db)
get_appropriate_menu = lambda user_id, lang: handler_get_appropriate_menu(user_id, lang, db, main_menu_seller, main_menu_customer)

# Устанавливаем первого админа при старте
if ADMIN_ID > 0:
    try:
        # Проверяем существует ли пользователь
        user = db.get_user(ADMIN_ID)
        if not user:
            # Создаём пользователя-админа
            db.add_user(ADMIN_ID, "admin", "Admin")
        # Делаем админом
        db.set_admin(ADMIN_ID)
        print(f"✅ Админ установлен: {ADMIN_ID}")
    except Exception as e:
        print(f"⚠️ Ошибка при установке админа: {e}")

# Register modular handlers from handlers package
from handlers import registration, user_commands, admin

# Setup registration handlers
registration.setup(dp, db, get_text, get_cities, city_keyboard, main_menu_customer,
                  validator, rate_limiter, logger, secure_user_input)

# Setup user command handlers
user_commands.setup(dp, db, get_text, get_cities, city_keyboard, language_keyboard,
                   phone_request_keyboard, main_menu_seller, main_menu_customer)

# Setup admin handlers
admin.setup(dp, db, get_text, admin_menu)

# Register middleware for registration check
dp.update.middleware(RegistrationCheckMiddleware(db, get_text, phone_request_keyboard))

# ============== REMAINING HANDLERS (TO BE MIGRATED) ==============
# Note: The handlers below will be gradually moved to the handlers/ package
# Handlers already migrated: registration, user_commands (start, language, cancel), admin (main panel)

# Skip duplicate handlers that are now in handler modules
# - Removed: Registration handlers (process_phone, process_city) - now in handlers/registration.py
# - Removed: User commands (cmd_start, choose_language, cancel_action, etc.) - now in handlers/user_commands.py
# - Removed: Admin commands (cmd_admin, admin_dashboard, admin_exit) - now in handlers/admin.py

from aiogram.types import Update
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable

# Middleware class is now imported from handlers.common, but keeping the old code commented for reference
# class RegistrationCheckMiddleware(BaseMiddleware):
#     """Проверяет, что пользователь зарегистрирован (есть номер телефона) перед любым действием"""
# (Implementation removed - now in handlers/common.py)

# Old middleware registration removed - now registered above with imported class

# ============== HANDLERS BELOW WILL BE GRADUALLY MIGRATED ==============
# The following handlers remain in bot.py and can be moved to handler modules incrementally:
# - Store registration and management
# - Offer creation and management
# - Booking operations
# - Callback handlers (pagination, filters, etc.)
# - Additional admin handlers (moderation, detailed stats, etc.)

# ============== MY CITY HANDLER (TODO: Already in user_commands.py, remove after testing) ==============
# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(F.text == "Мой город")
# async def my_city(message: types.Message, state: FSMContext = None):
#     user_id = message.from_user.id
#     lang = db.get_user_language(user_id)
#     user = db.get_user(user_id)
#     current_city = user[4] if user and len(user) > 4 else None
#     if not current_city:
#         current_city = get_cities(lang)[0]
    
#     # Получаем статистику по городу
#     stats_text = ""
#     try:
#         stores_count = len(db.get_stores_by_city(current_city))
#         offers_count = len(db.get_active_offers(city=current_city))
#         stats_text = f"\n\n📊 В вашем городе:\n🏪 Магазинов: {stores_count}\n🍽 Предложений: {offers_count}"
#     except:
#         pass
    
#     # Создаём inline-клавиатуру с кнопками
#     builder = InlineKeyboardBuilder()
#     builder.button(
#         text="✏️ Изменить город" if lang == 'ru' else "✏️ Shaharni o'zgartirish",
#         callback_data="change_city"
#     )
#     builder.button(
#         text="◀️ Назад" if lang == 'ru' else "◀️ Orqaga",
#         callback_data="back_to_menu"
#     )
#     builder.adjust(1)
    
#     await message.answer(
#         f"{get_text(lang, 'your_city')}: {current_city}{stats_text}",
#         reply_markup=builder.as_markup(),
#         parse_mode="HTML"
#     )

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.callback_query(F.data == "change_city")
# async def show_city_selection(callback: types.CallbackQuery, state: FSMContext):
#     """Показать список городов для выбора"""
#     lang = db.get_user_language(callback.from_user.id)
#     await callback.message.edit_text(
#         get_text(lang, 'choose_city'),
#         reply_markup=city_keyboard(lang)
#     )
#     await callback.answer()

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.callback_query(F.data == "back_to_menu")
# async def back_to_main_menu(callback: types.CallbackQuery):
#     """Вернуться в главное меню"""
#     lang = db.get_user_language(callback.from_user.id)
#     user = db.get_user(callback.from_user.id)
#     menu = main_menu_seller(lang) if user and user[6] == "seller" else main_menu_customer(lang)
    
#     await callback.message.delete()
#     await callback.message.answer(
#         get_text(lang, 'main_menu') if 'main_menu' in dir() else "Главное меню",
#         reply_markup=menu
#     )
#     await callback.answer()

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(F.text.in_(get_cities('ru') + get_cities('uz')))
# async def change_city(message: types.Message, state: FSMContext = None):
#     """Обработчик быстрой смены города (без FSM состояния)"""
#     user_id = message.from_user.id
#     lang = db.get_user_language(user_id)
#     user = db.get_user(user_id)
    
#     # ВАЖНО: Проверяем текущее состояние FSM
#     # Если пользователь в процессе регистрации (магазина или самого себя), пропускаем
#     if state:
#         current_state = await state.get_state()
#         if current_state and (current_state.startswith('RegisterStore:') or current_state.startswith('Registration:')):
#             # Пользователь в процессе регистрации — не трогаем, пусть обработает соответствующий обработчик
#             return
    
#     new_city = message.text
    
#     # Сохраняем новый город
#     db.update_user_city(user_id, new_city)
    
#     # Получаем обновлённое главное меню
#     menu = main_menu_seller(lang) if user and user[6] == "seller" else main_menu_customer(lang)
    
#     await message.answer(
#         f"✅ {get_text(lang, 'city_changed', city=new_city)}\n\n"
#         f"Теперь вы будете видеть предложения из города {new_city}",
#         reply_markup=menu,
#         parse_mode="HTML"
#     )

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(Command("start"))
# async def cmd_start(message: types.Message, state: FSMContext):
#     user = db.get_user(message.from_user.id)
    
#     if not user:
#         # Новый пользователь - показываем выбор языка
#         # НЕ создаём пользователя до получения номера телефона!
#         await message.answer(
#             get_text('ru', 'choose_language'),
#             reply_markup=language_keyboard()
#         )
#         return
    
#     lang = db.get_user_language(message.from_user.id)
    
#     # Проверка телефона
#     if not user[3]:
#         await message.answer(
#             get_text(lang, 'welcome', name=message.from_user.first_name),
#             parse_mode="HTML",
#             reply_markup=phone_request_keyboard(lang)
#         )
#         await state.set_state(Registration.phone)
#         return
    
#     # Проверка города
#     if not user[4]:
#         await message.answer(
#             get_text(lang, 'choose_city'),
#             parse_mode="HTML",
#             reply_markup=city_keyboard(lang, allow_cancel=False)
#         )
#         await state.set_state(Registration.city)
#         return
    
#     # Приветствие
#     menu = main_menu_seller(lang) if user[6] == "seller" else main_menu_customer(lang)
#     await message.answer(
#         get_text(lang, 'welcome_back', name=message.from_user.first_name, city=user[4]),
#         parse_mode="HTML",
#         reply_markup=menu
#     )

# # ============== АДМИН ПАНЕЛЬ ==============

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(Command("admin"))
# async def cmd_admin(message: types.Message):
#     lang = db.get_user_language(message.from_user.id)
    
#     if not db.is_admin(message.from_user.id):
#         await message.answer(get_text(lang, 'no_admin_access'))
#         return
    
#     await message.answer(
#         "👑 <b>Админ-панель Fudly</b>\n\n"
#         "Добро пожаловать! Выберите раздел:",
#         parse_mode="HTML",
#         reply_markup=admin_menu()
#     )

# # ============== АДМИН ПАНЕЛЬ - DASHBOARD ==============

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(F.text == "📊 Dashboard")
# async def admin_dashboard(message: types.Message):
#     """Главная панель с общей статистикой и быстрыми действиями"""
#     if not db.is_admin(message.from_user.id):
#         return
    
#     conn = db.get_connection()
#     cursor = conn.cursor()
    
#     # Общая статистика
#     cursor.execute('SELECT COUNT(*) FROM users')
#     total_users = cursor.fetchone()[0]
    
#     cursor.execute('SELECT COUNT(*) FROM users WHERE role = "seller"')
#     sellers = cursor.fetchone()[0]
    
#     cursor.execute('SELECT COUNT(*) FROM users WHERE role = "customer"')
#     customers = cursor.fetchone()[0]
    
#     # Магазины
#     cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "active"')
#     active_stores = cursor.fetchone()[0]
    
#     cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "pending"')
#     pending_stores = cursor.fetchone()[0]
    
#     # Товары
#     cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "active"')
#     active_offers = cursor.fetchone()[0]
    
#     cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "inactive"')
#     inactive_offers = cursor.fetchone()[0]
    
#     # Бронирования
#     cursor.execute('SELECT COUNT(*) FROM bookings')
#     total_bookings = cursor.fetchone()[0]
    
#     cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "pending"')
#     pending_bookings = cursor.fetchone()[0]
    
#     # Статистика за сегодня (узбекское время)
#     today = get_uzb_time().strftime('%Y-%m-%d')
    
#     cursor.execute('SELECT COUNT(*) FROM bookings WHERE DATE(created_at) = ?', (today,))
#     today_bookings = cursor.fetchone()[0]
    
#     cursor.execute('''
#         SELECT SUM(o.discount_price * b.quantity)
#         FROM bookings b
#         JOIN offers o ON b.offer_id = o.offer_id
#         WHERE DATE(b.created_at) = ? AND b.status != 'cancelled'
#     ''', (today,))
#     today_revenue = cursor.fetchone()[0] or 0
    
#     # Новые пользователи за сегодня
#     cursor.execute('''
#         SELECT COUNT(*) FROM users 
#         WHERE DATE(created_at) = ?
#     ''', (today,))
#     today_users = cursor.fetchone()[0]
    
#     conn.close()
    
#     # Формируем сообщение
#     text = "📊 <b>Dashboard - Общая статистика</b>\n\n"
    
#     text += "👥 <b>Пользователи:</b>\n"
#     text += f"├ Всего: {total_users} (+{today_users} сегодня)\n"
#     text += f"├ 🏪 Партнёры: {sellers}\n"
#     text += f"└ 🛍 Покупатели: {customers}\n\n"
    
#     text += "🏪 <b>Магазины:</b>\n"
#     text += f"├ ✅ Активные: {active_stores}\n"
#     text += f"└ ⏳ На модерации: {pending_stores}\n\n"
    
#     text += "📦 <b>Товары:</b>\n"
#     text += f"├ ✅ Активные: {active_offers}\n"
#     text += f"└ ❌ Неактивные: {inactive_offers}\n\n"
    
#     text += "🎫 <b>Бронирования:</b>\n"
#     text += f"├ Всего: {total_bookings}\n"
#     text += f"├ ⏳ Активные: {pending_bookings}\n"
#     text += f"└ 📅 Сегодня: {today_bookings}\n\n"
    
#     text += f"💰 <b>Выручка сегодня:</b> {int(today_revenue):,} сум"
    
#     # Inline-кнопки для быстрых действий
#     from aiogram.utils.keyboard import InlineKeyboardBuilder
#     kb = InlineKeyboardBuilder()
    
#     if pending_stores > 0:
#         kb.button(text=f"⏳ Модерация ({pending_stores})", callback_data="admin_moderation")
    
#     kb.button(text="📊 Детальная статистика", callback_data="admin_detailed_stats")
#     kb.button(text="🔄 Обновить", callback_data="admin_refresh_dashboard")
#     kb.adjust(1)
    
#     await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

# # ============== АДМИН ПАНЕЛЬ - ОБРАБОТЧИКИ ==============

@dp.message(F.text == "👥 Пользователи")
async def admin_users(message: types.Message):
    """Статистика пользователей с inline-меню"""
    if not db.is_admin(message.from_user.id):
        return
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM users')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "seller"')
    sellers = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "customer"')
    customers = cursor.fetchone()[0]
    
    # За последние 7 дней
    cursor.execute('''
        SELECT COUNT(*) FROM users 
        WHERE DATE(created_at) >= DATE('now', '-7 days')
    ''')
    week_users = cursor.fetchone()[0]
    
    # За сегодня
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    cursor.execute('SELECT COUNT(*) FROM users WHERE DATE(created_at) = ?', (today,))
    today_users = cursor.fetchone()[0]
    
    conn.close()
    
    text = "👥 <b>Пользователи</b>\n\n"
    text += f"📊 Всего: {total}\n"
    text += f"├ 🏪 Партнёры: {sellers}\n"
    text += f"└ 🛍 Покупатели: {customers}\n\n"
    text += f"📅 За неделю: +{week_users}\n"
    text += f"📅 Сегодня: +{today_users}"
    
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Список партнёров", callback_data="admin_list_sellers")
    kb.button(text="🔍 Поиск пользователя", callback_data="admin_search_user")
    kb.adjust(1)
    
    await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

@dp.message(F.text == "🏪 Магазины")
async def admin_stores(message: types.Message):
    """Управление магазинами с inline-меню"""
    if not db.is_admin(message.from_user.id):
        return
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "active"')
    active = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "pending"')
    pending = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "rejected"')
    rejected = cursor.fetchone()[0]
    
    conn.close()
    
    text = "🏪 <b>Магазины</b>\n\n"
    text += f"✅ Активные: {active}\n"
    text += f"⏳ На модерации: {pending}\n"
    text += f"❌ Отклонённые: {rejected}"
    
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    
    if pending > 0:
        kb.button(text=f"⏳ Модерация ({pending})", callback_data="admin_moderation")
    
    kb.button(text="✅ Одобренные", callback_data="admin_approved_stores")
    kb.button(text="❌ Отклонённые", callback_data="admin_rejected_stores")
    kb.button(text="🔍 Поиск магазина", callback_data="admin_search_store")
    kb.adjust(1)
    
    await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

@dp.message(F.text == "📦 Товары")
async def admin_offers(message: types.Message):
    """Статистика товаров"""
    if not db.is_admin(message.from_user.id):
        return
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "active"')
    active = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "inactive"')
    inactive = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "deleted"')
    deleted = cursor.fetchone()[0]
    
    # Топ категорий
    cursor.execute('''
        SELECT category, COUNT(*) as cnt 
        FROM offers 
        WHERE status = 'active' AND category IS NOT NULL
        GROUP BY category 
        ORDER BY cnt DESC 
        LIMIT 5
    ''')
    top_categories = cursor.fetchall()
    
    conn.close()
    
    text = "📦 <b>Товары</b>\n\n"
    text += f"✅ Активные: {active}\n"
    text += f"❌ Неактивные: {inactive}\n"
    text += f"🗑 Удалённые: {deleted}\n\n"
    
    if top_categories:
        text += "<b>Топ категорий:</b>\n"
        for cat, cnt in top_categories:
            text += f"├ {cat}: {cnt}\n"
    
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Все активные", callback_data="admin_all_offers")
    kb.button(text="🗑 Очистить старые", callback_data="admin_cleanup_offers")
    kb.adjust(1)
    
    await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

@dp.message(F.text == "📋 Бронирования")
async def admin_bookings(message: types.Message):
    """Статистика бронирований"""
    if not db.is_admin(message.from_user.id):
        return
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM bookings')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "pending"')
    pending = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "completed"')
    completed = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "cancelled"')
    cancelled = cursor.fetchone()[0]
    
    # За сегодня
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE DATE(created_at) = ?', (today,))
    today_bookings = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT SUM(o.discount_price * b.quantity)
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE DATE(b.created_at) = ? AND b.status != 'cancelled'
    ''', (today,))
    today_revenue = cursor.fetchone()[0] or 0
    
    conn.close()
    
    text = "🎫 <b>Бронирования</b>\n\n"
    text += f"📊 Всего: {total}\n"
    text += f"├ ⏳ Активные: {pending}\n"
    text += f"├ ✅ Завершённые: {completed}\n"
    text += f"└ ❌ Отменённые: {cancelled}\n\n"
    text += f"📅 Сегодня: {today_bookings}\n"
    text += f"� Выручка: {int(today_revenue):,} сум"
    
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Активные", callback_data="admin_pending_bookings")
    kb.button(text="✅ Завершённые", callback_data="admin_completed_bookings")
    kb.button(text="📊 Статистика", callback_data="admin_bookings_stats")
    kb.adjust(1)
    
    await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(F.text == "🔙 Выход")
# async def admin_exit(message: types.Message):
#     """Выход из админ-панели"""
#     lang = db.get_user_language(message.from_user.id)
#     user = db.get_user(message.from_user.id)
#     menu = main_menu_seller(lang) if user and user[6] == "seller" else main_menu_customer(lang)
#     await message.answer(
#         "👋 Выход из админ-панели",
#         reply_markup=menu
#     )

# # ============== ВЫБОР ЯЗЫКА ==============

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.callback_query(F.data.startswith("lang_"))
# async def choose_language(callback: types.CallbackQuery, state: FSMContext):
#     lang = callback.data.split("_")[1]
    
#     # Показываем меню после выбора языка
#     user = db.get_user(callback.from_user.id)
    
#     # ПРОВЕРКА: если пользователя нет в БД (новый пользователь)
#     if not user:
#         # Создаём нового пользователя С выбранным языком
#         db.add_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
#         db.update_user_language(callback.from_user.id, lang)
#         await callback.message.edit_text(get_text(lang, 'language_changed'))
#         await callback.message.answer(
#             get_text(lang, 'welcome', name=callback.from_user.first_name),
#             parse_mode="HTML",
#             reply_markup=phone_request_keyboard(lang)
#         )
#         await state.set_state(Registration.phone)
#         return
    
#     # Если пользователь уже есть — просто обновляем язык
#     db.update_user_language(callback.from_user.id, lang)
#     await callback.message.edit_text(get_text(lang, 'language_changed'))
    
#     # Если нет телефона - запрашиваем
#     if not user[3]:
#         await callback.message.answer(
#             get_text(lang, 'welcome', name=callback.from_user.first_name),
#             parse_mode="HTML",
#             reply_markup=phone_request_keyboard(lang)
#         )
#         await state.set_state(Registration.phone)
#         return
    
#     # Если нет города - запрашиваем
#     if not user[4]:
#         await callback.message.answer(
#             get_text(lang, 'choose_city'),
#             parse_mode="HTML",
#             reply_markup=city_keyboard(lang, allow_cancel=False)
#         )
#         await state.set_state(Registration.city)
#         return
    
#     # Показываем главное меню
#     menu = main_menu_seller(lang) if user[6] == "seller" else main_menu_customer(lang)
#     await callback.message.answer(
#         get_text(lang, 'welcome_back', name=callback.from_user.first_name, city=user[4]),
#         parse_mode="HTML",
#         reply_markup=menu
#     )

# # ============== ОТМЕНА ДЕЙСТВИЙ ==============

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(F.text.contains("Отмена") | F.text.contains("Bekor qilish"))
# async def cancel_action(message: types.Message, state: FSMContext):
#     lang = db.get_user_language(message.from_user.id)
#     current_state = await state.get_state()
    
#     # БЛОКИРУЕМ отмену обязательной регистрации
#     if current_state in ['Registration:phone', 'Registration:city']:
#         user = db.get_user(message.from_user.id)
#         # Если нет номера телефона — регистрация обязательна, отмена запрещена
#         if not user or not user[3]:
#             await message.answer(
#                 "❌ Регистрация обязательна для использования бота.\n\n"
#                 "📱 Пожалуйста, поделитесь номером телефона.",
#                 reply_markup=phone_request_keyboard(lang)
#             )
#             return
    
#     # Для всех остальных состояний — разрешаем отмену
#     await state.clear()

#     # Map state group to preferred menu context
#     seller_groups = {"RegisterStore", "CreateOffer", "BulkCreate", "ConfirmOrder"}
#     customer_groups = {"Registration", "BookOffer", "ChangeCity"}

#     preferred_menu = None
#     if current_state:
#         try:
#             state_group = str(current_state).split(":", 1)[0]
#             if state_group in seller_groups:
#                 preferred_menu = "seller"
#             elif state_group in customer_groups:
#                 preferred_menu = "customer"
#         except Exception:
#             preferred_menu = None

#     user = db.get_user(message.from_user.id)
#     role = user[6] if user and len(user) > 6 else "customer"
    
#     # КРИТИЧНО: При отмене RegisterStore ВСЕГДА возвращаем на меню клиента
#     # потому что у пользователя ещё НЕТ одобренного магазина
#     if current_state and str(current_state).startswith("RegisterStore"):
#         # Отменяем регистрацию магазина - возвращаем на клиентское меню
#         await message.answer(
#             get_text(lang, 'operation_cancelled'),
#             reply_markup=main_menu_customer(lang)
#         )
#         return
    
#     # ВАЖНО: Проверяем наличие одобренного магазина для партнёров
#     if role == "seller":
#         # Используем готовую функцию has_approved_store для проверки
#         if not has_approved_store(message.from_user.id):
#             # Нет одобренного магазина — показываем меню покупателя
#             role = "customer"
#             preferred_menu = "customer"
    
#     # View mode override has priority if set
#     view_override = user_view_mode.get(message.from_user.id)
#     target = preferred_menu or view_override or ("seller" if role == "seller" else "customer")
#     menu = main_menu_seller(lang) if target == "seller" else main_menu_customer(lang)

#     await message.answer(
#         get_text(lang, 'operation_cancelled'),
#         reply_markup=menu
#     )

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.callback_query(F.data == "cancel_offer")
# async def cancel_offer_callback(callback: types.CallbackQuery, state: FSMContext):
#     """Обработчик кнопки отмены создания товара"""
#     lang = db.get_user_language(callback.from_user.id)
#     await state.clear()
    
#     await callback.message.edit_text(
#         f"❌ {'Создание товара отменено' if lang == 'ru' else 'Mahsulot yaratish bekor qilindi'}",
#         parse_mode="HTML"
#     )
    
#     await callback.message.answer(
#         get_text(lang, 'operation_cancelled'),
#         reply_markup=main_menu_seller(lang)
#     )
    
#     await callback.answer()

# # ============== РЕГИСТРАЦИЯ ==============

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(Registration.phone, F.contact)
# async def process_phone(message: types.Message, state: FSMContext):
#     # Пользователь должен быть создан при выборе языка
#     lang = db.get_user_language(message.from_user.id)
#     phone = message.contact.phone_number
    
#     # Обновляем номер телефона
#     db.update_user_phone(message.from_user.id, phone)
    
#     await message.answer(
#         get_text(lang, 'choose_city'),
#         parse_mode="HTML",
#         reply_markup=city_keyboard(lang, allow_cancel=False)
#     )
#     await state.set_state(Registration.city)

# DUPLICATE HANDLER - Moved to handlers/ package
# @dp.message(Registration.city)
# @secure_user_input
# async def process_city(message: types.Message, state: FSMContext):
#     lang = db.get_user_language(message.from_user.id)
    
#     # Rate limiting check
#     try:
#         if not rate_limiter.is_allowed(message.from_user.id, 'city_selection', max_requests=5, window_seconds=60):
#             await message.answer(get_text(lang, 'rate_limit_exceeded'))
#             return
#     except Exception as e:
#         logger.warning(f"Rate limiter error: {e}")
    
#     cities = get_cities(lang)
#     city_text = validator.sanitize_text(message.text.replace("📍 ", "").strip())
    
#     # Validate city input
#     if not validator.validate_city(city_text):
#         await message.answer(get_text(lang, 'invalid_city'))
#         return
    
#     if city_text in cities:
#         db.update_user_city(message.from_user.id, city_text)
#         await state.clear()
#         await message.answer(
#             get_text(lang, 'city_changed', city=city_text),
#             reply_markup=main_menu_customer(lang)
#         )


# # ============== PAGINATION HELPERS ==============

# ITEMS_PER_PAGE = 10

def get_pagination_keyboard(lang: str, current_page: int, total_pages: int, callback_prefix: str):
    """Создать клавиатуру для пагинации"""
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    
    builder = InlineKeyboardBuilder()
    
    # Кнопки навигации
    buttons = []
    
    if current_page > 0:
        buttons.append(("◀️ Назад" if lang == 'ru' else "◀️ Orqaga", f"{callback_prefix}_page_{current_page - 1}"))
    
    # Показываем текущую страницу
    buttons.append((f"📄 {current_page + 1}/{total_pages}", "noop"))
    
    if current_page < total_pages - 1:
        buttons.append(("Вперёд ▶️" if lang == 'ru' else "Oldinga ▶️", f"{callback_prefix}_page_{current_page + 1}"))
    
    for text, callback in buttons:
        builder.button(text=text, callback_data=callback)
    
    builder.adjust(len(buttons))
    return builder.as_markup()

@dp.callback_query(F.data == "noop")
async def noop_callback(callback: types.CallbackQuery):
    """Заглушка для кнопки с номером страницы"""
    await callback.answer()

# ============== ДОСТУПНЫЕ ПРЕДЛОЖЕНИЯ ==============

@dp.message(F.text.contains("Доступные предложения") | F.text.contains("Mavjud takliflar"))
async def available_offers(message: types.Message):
    """Показать топ актуальные предложения в городе пользователя"""
    lang = db.get_user_language(message.from_user.id)
    user = db.get_user(message.from_user.id)
    
    if not user:
        await message.answer(get_text(lang, 'error'))
        return
    
    city = user[4]  # город пользователя
    search_city = normalize_city(city)  # Нормализуем для поиска в БД
    
    # Получаем топ 10 предложений с самыми большими скидками
    offers = db.get_top_offers_by_city(search_city, limit=10)
    
    if not offers:
        await message.answer(
            get_text(lang, 'no_offers_in_city') if 'no_offers_in_city' in dir() 
            else f"😔 В городе {city} пока нет активных предложений.\n\nМы уведомим вас, когда появятся новые скидки!"
        )
        return
    
    # Отправляем заголовок с фильтрами
    await message.answer(
        f"🔥 <b>{'ТОП ПРЕДЛОЖЕНИЯ' if lang == 'ru' else 'TOP TAKLIFLAR'}</b>\n"
        f"📍 {city}\n\n"
        f"{'Найдено' if lang == 'ru' else 'Topildi'}: {len(offers)} {'предложений' if lang == 'ru' else 'taklif'}\n"
        f"{'Выберите категорию для фильтрации:' if lang == 'ru' else 'Filtrlash uchun kategoriyani tanlang:'}",
        parse_mode="HTML",
        reply_markup=offers_category_filter(lang)
    )
    
    # Отправляем карточки предложений
    for offer in offers:
        await send_offer_card(message, offer, lang)
        await asyncio.sleep(0.1)  # Небольшая задержка между сообщениями

async def send_offer_card(message: types.Message, offer: tuple, lang: str):
    """Отправить карточку предложения"""
    # Распаковываем данные предложения
    # АКТУАЛЬНАЯ структура (после ALTER TABLE):
    # [0]=offer_id, [1]=store_id, [2]=title, [3]=description,
    # [4]=original_price, [5]=discount_price, [6]=quantity, [7]=available_from,
    # [8]=available_until, [9]=status, [10]=photo, [11]=created_at,
    # [12]=expiry_date, [13]=unit, [14]=category
    # После JOIN с stores: [15]=store_name, [16]=store_address, [17]=store_city,
    # [18]=store_category, [19]=discount_percent (если добавлено в запросе)
    
    offer_id = offer[0]
    store_id = offer[1]
    product_name = offer[2]
    original_price = offer[4]
    discount_price = offer[5]
    quantity = offer[6]
    expiry_date = offer[12]  # ПРАВИЛЬНЫЙ индекс для expiry_date (после ALTER TABLE)
    store_name = offer[15] if len(offer) > 15 else "Магазин"
    store_address = offer[16] if len(offer) > 16 else ""
    store_category = offer[18] if len(offer) > 18 else ""
    discount_percent = offer[19] if len(offer) > 19 else 0
    
    # Формируем текст карточки
    text = f"🔥 <b>{product_name}</b>\n\n"
    text += f"💰 Было: {original_price:,.0f} сум\n"
    text += f"💵 Сейчас: <b>{discount_price:,.0f} сум</b>\n"
    text += f"📊 Скидка: <b>-{discount_percent:.0f}%</b>\n\n"
    text += f"🏪 {store_name}\n"
    text += f"🏷 {store_category}\n"
    if store_address:
        text += f"📍 {store_address}\n"
    text += f"📦 Осталось: {quantity} шт\n"
    text += f"⏰ До: {expiry_date[:10]}"
    
    # Кнопки
    builder = InlineKeyboardBuilder()
    builder.button(text="🛒 Забронировать" if lang == 'ru' else "🛒 Bron qilish", 
                   callback_data=f"book_{offer_id}")
    builder.button(text="🏪 О магазине" if lang == 'ru' else "🏪 Dokon haqida", 
                   callback_data=f"store_info_{store_id}")
    builder.adjust(1)
    
    await message.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())

@dp.callback_query(F.data == "offers_all")
async def show_all_offers(callback: types.CallbackQuery):
    """Показать все предложения без фильтра"""
    lang = db.get_user_language(callback.from_user.id)
    user = db.get_user(callback.from_user.id)
    
    if not user:
        await callback.answer(get_text(lang, 'error'), show_alert=True)
        return
    
    city = user[4]
    offers = db.get_top_offers_by_city(city, limit=20)
    
    if not offers:
        await callback.answer("😔 Нет доступных предложений", show_alert=True)
        return
    
    await callback.answer()
    
    # Обновляем сообщение с фильтрами
    await callback.message.edit_text(
        f"🔥 <b>{'ВСЕ ПРЕДЛОЖЕНИЯ' if lang == 'ru' else 'BARCHA TAKLIFLAR'}</b>\n"
        f"📍 {city}\n\n"
        f"{'Найдено' if lang == 'ru' else 'Topildi'}: {len(offers)} {'предложений' if lang == 'ru' else 'taklif'}",
        parse_mode="HTML",
        reply_markup=offers_category_filter(lang)
    )
    
    # Отправляем карточки
    for offer in offers[:10]:  # Показываем первые 10
        await send_offer_card(callback.message, offer, lang)
        await asyncio.sleep(0.1)

@dp.callback_query(F.data.startswith("offers_cat_"))
async def filter_offers_by_category(callback: types.CallbackQuery):
    """Фильтр предложений по категории"""
    lang = db.get_user_language(callback.from_user.id)
    user = db.get_user(callback.from_user.id)
    
    if not user:
        await callback.answer(get_text(lang, 'error'), show_alert=True)
        return
    
    city = user[4]
    cat_index = int(callback.data.split("_")[-1])
    categories = get_categories(lang)
    
    if cat_index >= len(categories):
        await callback.answer("Ошибка", show_alert=True)
        return
    
    category = categories[cat_index]
    category_normalized = normalize_category(category)
    
    # Получаем предложения по категории
    offers = db.get_offers_by_city_and_category(city, category_normalized, limit=20)
    
    if not offers:
        await callback.answer(f"😔 В категории {category} нет предложений", show_alert=True)
        return
    
    await callback.answer()
    
    # Обновляем сообщение
    await callback.message.edit_text(
        f"🔥 <b>{category.upper()}</b>\n"
        f"📍 {city}\n\n"
        f"{'Найдено' if lang == 'ru' else 'Topildi'}: {len(offers)} {'предложений' if lang == 'ru' else 'taklif'}",
        parse_mode="HTML",
        reply_markup=offers_category_filter(lang)
    )
    
    # Отправляем карточки
    for offer in offers[:10]:
        await send_offer_card(callback.message, offer, lang)
        await asyncio.sleep(0.1)

@dp.callback_query(F.data.startswith("filter_store_"))
async def filter_offers_by_store(callback: types.CallbackQuery):
    """Фильтр предложений по конкретному магазину"""
    lang = db.get_user_language(callback.from_user.id)
    store_id = int(callback.data.split("_")[-1])
    
    store = db.get_store(store_id)
    if not store:
        await callback.answer("❌ Магазин не найден", show_alert=True)
        return
    
    offers = db.get_active_offers(store_id=store_id)
    
    if not offers:
        await callback.answer(f"😔 В магазине {store[2]} нет активных предложений", show_alert=True)
        return
    
    await callback.answer()
    
    # Обновляем сообщение
    await callback.message.edit_text(
        f"🏪 <b>{store[2]}</b>\n"
        f"📍 {store[4]}\n\n"
        f"{'Найдено' if lang == 'ru' else 'Topildi'}: {len(offers)} {'предложений' if lang == 'ru' else 'taklif'}",
        parse_mode="HTML",
        reply_markup=offers_category_filter(lang)
    )
    
    # Отправляем карточки
    for offer in offers[:10]:
        await send_offer_card(callback.message, offer, lang)
        await asyncio.sleep(0.1)

@dp.callback_query(F.data == "filter_all")
async def show_all_offers_filter(callback: types.CallbackQuery):
    """Показать все предложения (альтернативный обработчик для filter_all)"""
    # Переадресуем на offers_all
    callback.data = "offers_all"
    await show_all_offers(callback)

@dp.callback_query(F.data.startswith("store_info_"))
async def show_store_info(callback: types.CallbackQuery):
    """Показать информацию о магазине"""
    lang = db.get_user_language(callback.from_user.id)
    store_id = int(callback.data.split("_")[-1])
    
    store = db.get_store(store_id)
    if not store:
        await callback.answer("Магазин не найден", show_alert=True)
        return
    
    avg_rating = db.get_store_average_rating(store_id)
    ratings_count = len(db.get_store_ratings(store_id))
    
    text = f"🏪 <b>{store[2]}</b>\n\n"
    text += f"🏷 {store[6]}\n"
    text += f"📍 {store[3]}, {store[4]}\n"
    text += f"📝 {store[5]}\n"
    text += f"📞 {store[7]}\n"
    text += f"⭐ Рейтинг: {avg_rating:.1f}/5 ({ratings_count} отзывов)"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🛍 Все товары магазина" if lang == 'ru' else "🛍 Barcha mahsulotlar", 
                   callback_data=f"store_{store_id}")
    builder.button(text="◀️ Назад" if lang == 'ru' else "◀️ Orqaga", 
                   callback_data="offers_all")
    builder.adjust(1)
    
    await callback.message.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())
    await callback.answer()

# ============== БРОНИРОВАНИЕ ==============

@dp.callback_query(F.data.startswith("cat_"))
async def select_category(callback: types.CallbackQuery):
    """Выбор категории заведения"""
    await callback.answer()  # Подтверждаем callback
    try:
        lang = db.get_user_language(callback.from_user.id)
        user = db.get_user(callback.from_user.id)
        
        if not user:
            await callback.answer(get_text(lang, 'error'), show_alert=True)
            return
        
        city = user[4]
        search_city = normalize_city(city)
        
        categories = get_categories(lang)
        cat_index = int(callback.data.split("_")[1])
        category = categories[cat_index]
        category = normalize_category(category)
        
        
        # Получаем магазины этой категории в городе пользователя
        stores = db.get_stores_by_category(category, search_city)
        
        
        if not stores:
            await callback.answer(get_text(lang, 'no_offers'), show_alert=True)
            return
        
        await callback.message.edit_text(
            get_text(lang, 'choose_store'),
            reply_markup=store_selection(stores, lang, cat_index=cat_index, offset=0)
        )
        await callback.answer()
    except Exception as e:
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data == "back_to_categories")
async def back_to_categories(callback: types.CallbackQuery):
    """Возврат к выбору категорий"""
    lang = db.get_user_language(callback.from_user.id)
    
    await callback.message.edit_text(
        get_text(lang, 'choose_category'),
        reply_markup=store_category_selection(lang)
    )
    await callback.answer()

@dp.callback_query(F.data.regex(r"^stores_(next|prev)_\d+_\d+$"))
async def stores_pagination(callback: types.CallbackQuery):
    """Пагинация списка магазинов по выбранной категории"""
    try:
        lang = db.get_user_language(callback.from_user.id)
        user = db.get_user(callback.from_user.id)
        
        if not user:
            await callback.answer(get_text(lang, 'error'), show_alert=True)
            return
        
        city = user[4]
        search_city = normalize_city(city)
        
        parts = callback.data.split("_")  # stores_next_{catIndex}_{offset}
        _action = parts[1]
        cat_index = int(parts[2])
        offset = int(parts[3])

        categories = get_categories(lang)
        if cat_index < 0 or cat_index >= len(categories):
            await callback.answer("Ошибка", show_alert=True)
            return
        category_label = categories[cat_index]
        category = normalize_store_category(category_label)  # ИСПРАВЛЕНО: было normalize_category
        stores = db.get_stores_by_category(category, search_city)

        # Обновляем только клавиатуру
        await callback.message.edit_reply_markup(
            reply_markup=store_selection(stores, lang, cat_index=cat_index, offset=offset)
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"stores_pagination error: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith("store_") and c.data.split("_")[0] == "store" and len(c.data.split("_")) == 2)
async def select_store(callback: types.CallbackQuery):
    """Выбор магазина"""
    try:
        lang = db.get_user_language(callback.from_user.id)
        store_id = int(callback.data.split("_")[1])
        
        # Extra debug to console and logger
        
        # Получаем предложения этого магазина
        offers = db.get_offers_by_store(store_id)
        
        print(f"select_store: offers count = {len(offers)}")
        
        if not offers:
            await callback.answer(get_text(lang, 'no_offers'), show_alert=True)
            return
        
        await callback.message.edit_text(
            get_text(lang, 'choose_offer'),
            reply_markup=offer_selection(offers, lang, store_id=store_id, offset=0)
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"ERROR in select_store: {e}", exc_info=True)
        await callback.answer("Ошибка при загрузке предложений", show_alert=True)

@dp.callback_query(F.data == "back_to_stores")
async def back_to_stores(callback: types.CallbackQuery):
    """Возврат к выбору магазинов"""
    # Для простоты вернём к категориям
    lang = db.get_user_language(callback.from_user.id)
    
    await callback.message.edit_text(
        get_text(lang, 'choose_category'),
        reply_markup=store_category_selection(lang)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("back_to_offers_"))
async def back_to_offers(callback: types.CallbackQuery):
    """Возврат к списку предложений магазина"""
    try:
        lang = db.get_user_language(callback.from_user.id)
        store_id = int(callback.data.split("_")[-1])
        
        # Получаем предложения этого магазина
        offers = db.get_offers_by_store(store_id)
        
        if not offers:
            await callback.answer(get_text(lang, 'no_offers'), show_alert=True)
            return
        
        await callback.message.edit_text(
            get_text(lang, 'choose_offer'),
            reply_markup=offer_selection(offers, lang, store_id=store_id, offset=0)
        )
    except Exception as e:
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.regex(r"^offers_(next|prev)_\d+_\d+$"))
async def offers_pagination(callback: types.CallbackQuery):
    """Пагинация списка предложений магазина"""
    try:
        lang = db.get_user_language(callback.from_user.id)
        parts = callback.data.split("_")  # offers_next_{store_id}_{offset}
        direction = parts[1]
        store_id = int(parts[2])
        offset = int(parts[3])

        offers = db.get_offers_by_store(store_id)
        # Перестраиваем клавиатуру с новым offset
        await callback.message.edit_reply_markup(
            reply_markup=offer_selection(offers, lang, store_id=store_id, offset=offset)
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"offers_pagination error: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("offer_"), ~F.data.startswith("offer_skip") & ~F.data.startswith("offer_no"))
async def select_offer(callback: types.CallbackQuery):
    """Показ деталей предложения"""
    try:
        lang = db.get_user_language(callback.from_user.id)
        offer_id = int(callback.data.split("_")[1])
        
        
        offer = db.get_offer(offer_id)
        if not offer:
            await callback.answer(get_text(lang, 'no_offers'), show_alert=True)
            return
        
        # Показываем детали как в старом коде
        discount_percent = int((1 - offer[5] / offer[4]) * 100) if offer[4] and offer[4] > 0 else 0
        
        text = f"🍽 <b>{offer[2]}</b>\n"
        text += f"📝 {offer[3]}\n\n"
        text += f"💰 {int(offer[4]):,} ➜ <b>{int(offer[5]):,} {get_text(lang, 'currency')}</b> (-{discount_percent}%)\n"
        
        # unit находится в offers[13] (после ALTER TABLE), но после JOIN [13] это store_name
        # Проверяем длину: если len(offer) == 19, то [13]=unit, [14]=category, [15]=store_name...
        # Если len(offer) == 18, то нет unit/category, [13]=store_name...
        if len(offer) >= 19:
            # Есть unit и category поля
            unit = offer[13] if offer[13] else get_text(lang, 'unit')
        else:
            # Старая структура без unit/category
            unit = get_text(lang, 'unit')
        text += f"📦 {get_text(lang, 'available')}: {offer[6]} {unit}\n"
        text += f"🕐 {get_text(lang, 'time')}: {offer[7]} - {offer[8]}\n"
        
        # Таймер срока годности
        # АКТУАЛЬНАЯ структура: [12]=expiry_date, [9]=status, [10]=photo, [11]=created_at
        if len(offer) > 12 and offer[12]:
            time_remaining = db.get_time_remaining(offer[12])
            if time_remaining:
                text += f"{time_remaining}\n"
            text += f"📅 {get_text(lang, 'expires_on')}: {offer[12]}\n"
        
        if len(offer) > 16:
            # Магазин и адрес (после JOIN)
            text += f"🏪 {offer[13]}\n"  # store_name
            text += f"📍 {offer[14]}, {offer[15]}"  # address, city
        
        # Получаем store_id для кнопки "Назад"
        store_id = offer[1]  # store_id на позиции 1
        
        # Создаем клавиатуру с кнопкой "Назад"
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text=get_text(lang, 'book'), callback_data=f"book_{offer[0]}")
        keyboard.button(text=get_text(lang, 'back'), callback_data=f"back_to_offers_{store_id}")
        keyboard.adjust(1)
        
        # Удаляем клавиатуру и показываем детали с кнопкой бронирования
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard.as_markup()
        )
        await callback.answer()
    except Exception as e:
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("book_"))
async def book_offer_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало бронирования - спрашиваем количество"""
    lang = db.get_user_language(callback.from_user.id)
    # Rate limit booking start
    if not can_proceed(callback.from_user.id, "book_start"):
        await callback.answer(get_text(lang, 'operation_cancelled'), show_alert=True)
        return
    offer_id = int(callback.data.split("_")[1])
    offer = db.get_offer(offer_id)
    
    if not offer or offer[6] <= 0:
        await callback.answer(get_text(lang, 'no_offers'), show_alert=True)
        return
    
    # Сохраняем offer_id в состояние
    await state.update_data(offer_id=offer_id)
    await state.set_state(BookOffer.quantity)
    
    # Спрашиваем количество
    await callback.message.answer(
        f"🍽 <b>{offer[2]}</b>\n\n"
        f"📦 Доступно: {offer[6]} шт.\n"
        f"💰 Цена за 1 шт: {int(offer[5]):,} сум\n\n"
        f"Сколько вы хотите забронировать? (1-{offer[6]})",
        parse_mode="HTML",
        reply_markup=cancel_keyboard(lang)
    )
    await callback.answer()

@dp.message(BookOffer.quantity)
async def book_offer_quantity(message: types.Message, state: FSMContext):
    """Обработка количества и создание бронирования"""
    lang = db.get_user_language(message.from_user.id)
    # Rate limit booking confirm
    if not can_proceed(message.from_user.id, "book_confirm"):
        await message.answer(get_text(lang, 'operation_cancelled'))
        return
    
    try:
        quantity = int(message.text)
        if quantity < 1:
            await message.answer("❌ Количество должно быть больше 0")
            return
        
        data = await state.get_data()
        offer_id = data['offer_id']
        offer = db.get_offer(offer_id)
        
        if not offer or offer[6] < quantity:
            await message.answer(f"❌ Доступно только {offer[6]} шт.")
            return
        
        # Пытаемся атомарно забронировать товар и создать бронирование
        ok, booking_id, code = db.create_booking_atomic(offer_id, message.from_user.id, quantity)
        if not ok or booking_id is None or code is None:
            await message.answer("❌ К сожалению, выбранное количество уже недоступно. Обновите список предложений.")
            await state.clear()
            return
        try:
            METRICS["bookings_created"] += 1
        except Exception:
            pass
        
        await state.clear()
        
        # Уведомление партнёру с inline-кнопками quick actions
        store = db.get_store(offer[1])
        if store:
            partner_lang = db.get_user_language(store[1])
            # Получаем телефон клиента для партнёра
            customer = db.get_user(message.from_user.id)
            customer_phone = customer[3] if customer and customer[3] else "Не указан"
            
            # Создаём inline-клавиатуру для быстрых действий
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            notification_kb = InlineKeyboardBuilder()
            notification_kb.button(text="✅ Выдано", callback_data=f"complete_booking_{booking_id}")
            notification_kb.button(text="❌ Отменить", callback_data=f"cancel_booking_{booking_id}")
            notification_kb.adjust(2)
            
            try:
                await bot.send_message(
                    store[1],
                    f"🔔 <b>Новое бронирование!</b>\n\n"
                    f"🏪 {store[2]}\n"
                    f"🍽 {offer[2]}\n"
                    f"📦 Количество: {quantity} шт.\n"
                    f"👤 {message.from_user.first_name}\n"
                    f"📱 Телефон: <code>{customer_phone}</code>\n"
                    f"🎫 <code>{code}</code>\n"
                    f"💰 {int(offer[5] * quantity):,} сум",
                    parse_mode="HTML",
                    reply_markup=notification_kb.as_markup()
                )
            except Exception:
                pass
        
        total_price = int(offer[5] * quantity)
        # Структура offer после JOIN:
        # [0-12] базовые поля offers (13 полей)
        # [13]unit, [14]category (если добавлены через ALTER TABLE - 2 поля)
        # [15]store_name, [16]address, [17]city, [18]category (из stores JOIN - 4 поля)
        # Итого: если len(offer) == 19, то есть unit/category
        #        если len(offer) == 17, то нет unit/category
        
        if len(offer) >= 19:
            # Новая структура с unit и category
            store_name_idx = 15
            address_idx = 16
            city_idx = 17
        else:
            # Старая структура без unit/category
            store_name_idx = 13
            address_idx = 14
            city_idx = 15
        
        store_name = offer[store_name_idx] if len(offer) > store_name_idx else "Магазин"
        address = offer[address_idx] if len(offer) > address_idx else ""
        city = offer[city_idx] if len(offer) > city_idx else ""
        
        text = get_text(lang, 'booking_success',
                       store_name=store_name,
                       offer_name=offer[2],
                       price=f"{total_price:,}",
                       city=city,
                       address=address,
                       time=offer[8],
                       code=code)
        text += f"\n📦 Количество: {quantity} шт."
        
        user = db.get_user(message.from_user.id)
        menu = main_menu_seller(lang) if user and user[6] == "seller" else main_menu_customer(lang)
        
        await message.answer(text, parse_mode="HTML", reply_markup=booking_keyboard(booking_id, lang))
        await message.answer("✅ Готово!", reply_markup=menu)
        
    except ValueError:
        await message.answer("❌ Введите число!")
    except Exception as e:
        logger.error(f"Error in book_offer_quantity: {e}")
        await message.answer("❌ Произошла ошибка при бронировании. Попробуйте снова.")
        await state.clear()

# ============== ПОДРОБНОСТИ ПРЕДЛОЖЕНИЯ ==============

@dp.callback_query(F.data.startswith("details_"))
async def offer_details(callback: types.CallbackQuery):
    """Показывает подробную информацию о предложении"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[1])
    
    # Получаем полную информацию о предложении
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT o.*, s.name as store_name, s.address, s.city, s.phone, s.description as store_desc
        FROM offers o 
        JOIN stores s ON o.store_id = s.store_id 
        WHERE o.offer_id = ?
    ''', (offer_id,))
    
    offer_data = cursor.fetchone()
    conn.close()
    
    if not offer_data:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    # Формируем детальную информацию
    # АКТУАЛЬНАЯ структура после ALTER TABLE:
    # [0]offer_id, [1]store_id, [2]title, [3]description, [4]original_price, [5]discount_price,
    # [6]quantity, [7]available_from, [8]available_until, [9]status, [10]photo, [11]created_at,
    # [12]expiry_date, [13]unit, [14]category
    # После JOIN со stores: [15]store_name, [16]address, [17]city, [18]phone, [19]store_desc
    
    discount_percent = int((1 - offer_data[5] / offer_data[4]) * 100)
    unit = offer_data[13] if len(offer_data) > 13 and offer_data[13] else 'шт'
    
    text = f"🍽 <b>{offer_data[2]}</b>\n\n"
    text += f"📝 <b>Описание:</b> {offer_data[3]}\n\n"
    text += f"💰 <b>Цена:</b> {int(offer_data[4]):,} ➜ <b>{int(offer_data[5]):,} сум</b> (-{discount_percent}%)\n"
    text += f"📦 <b>Доступно:</b> {offer_data[6]} {unit}\n"
    text += f"🕐 <b>Время забора:</b> {offer_data[7]} - {offer_data[8]}\n"
    
    # Показываем срок годности если есть (expiry_date на позиции 12, status на позиции 9)
    if len(offer_data) > 12 and offer_data[9] == 'active' and offer_data[12]:
        time_remaining = db.get_time_remaining(offer_data[12])
        if time_remaining:
            text += f"{time_remaining}\n"
    
    # Правильные индексы: 15-store_name, 16-address, 17-city, 18-phone, 19-store_desc
    text += f"\n🏪 <b>Магазин:</b> {offer_data[15]}\n"
    text += f"📍 <b>Адрес:</b> {offer_data[16]}, {offer_data[17]}\n"
    
    if offer_data[18]:  # phone
        text += f"📞 <b>Телефон:</b> {offer_data[18]}\n"
    
    if offer_data[19]:  # store description - правильный индекс
        text += f"ℹ️ <b>О магазине:</b> {offer_data[19]}\n"
    
    # Создаем клавиатуру только с кнопкой бронирования
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🛒 Забронировать", callback_data=f"book_{offer_id}")
    keyboard.adjust(1)
    
    # Если есть фото предложения (индекс 10 - поле photo)
    if len(offer_data) > 10 and offer_data[10] and str(offer_data[10]).strip():
        try:
            await callback.message.edit_media(
                media=types.InputMediaPhoto(
                    media=offer_data[10],
                    caption=text,
                    parse_mode="HTML"
                ),
                reply_markup=keyboard.as_markup()
            )
        except Exception as e:
            logger.error(f"Error editing photo: {e}")
            await callback.message.edit_text(
                text, 
                parse_mode="HTML",
                reply_markup=keyboard.as_markup()
            )
    else:
        await callback.message.edit_text(
            text, 
            parse_mode="HTML",
            reply_markup=keyboard.as_markup()
        )
    
    await callback.answer()

# ============== ТОВАРЫ МАГАЗИНА ==============

@dp.callback_query(F.data.startswith("show_offers_"))
async def show_store_offers(callback: types.CallbackQuery):
    """Показывает товары конкретного магазина"""
    lang = db.get_user_language(callback.from_user.id)
    store_id = int(callback.data.split("_")[2])
    
    # Получаем информацию о магазине
    store = db.get_store(store_id)
    if not store:
        await callback.answer("❌ Магазин не найден", show_alert=True)
        return
    
    # Получаем товары магазина
    offers = db.get_active_offers(store_id=store_id)
    print(f"show_store_offers: offers count = {len(offers)}")
    
    if not offers:
        text = f"🏪 <b>{store[2]}</b>\n\n😔 В этом магазине пока нет активных предложений"
        await callback.message.edit_text(text, parse_mode="HTML")
        await callback.answer()
        return
    
    # Показываем заголовок
    text = f"🏪 <b>{store[2]}</b>\n📍 {store[4]}\n\n🛍 <b>Доступные товары ({len(offers)}):</b>\n\n"
    
    # Добавляем первые 5 товаров в текст
    for i, offer in enumerate(offers[:5]):
        discount_percent = int((1 - offer[5] / offer[4]) * 100)
        # unit находится в [13] если есть, иначе используем по умолчанию
        # После JOIN структура: [0-12] offers базовые, [13]unit (если есть), [14]category (если есть)
        # [15] или [13]store_name, [16] или [14]address, [17] или [15]city
        if len(offer) >= 19:
            # Новая структура с unit/category
            unit = offer[13] if offer[13] else 'шт'
        else:
            # Старая структура без unit/category
            unit = 'шт'
        
        text += f"{i+1}. <b>{offer[2]}</b>\n"
        text += f"   💰 {int(offer[4]):,} ➜ {int(offer[5]):,} сум (-{discount_percent}%)\n"
        text += f"   📦 {offer[6]} {unit}\n"
        if len(offer) > 12 and offer[12]:
            text += f"   📅 До: {offer[12]}\n"
        text += "\n"
    
    if len(offers) > 5:
        text += f"... и еще {len(offers) - 5} товаров"
    
    # Создаем кнопки для каждого товара
    keyboard = InlineKeyboardBuilder()
    for i, offer in enumerate(offers[:6]):  # Показываем кнопки для первых 6 товаров
        keyboard.button(text=f"📦 {offer[2][:20]}...", callback_data=f"details_{offer[0]}")
    keyboard.adjust(2)
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard.as_markup())
    await callback.answer()

# Удалён дубль обработчика store_*, чтобы избежать конфликта с select_store

# ============== МОИ БРОНИРОВАНИЯ ==============

@dp.message(F.text.contains("Мои бронирования") | F.text.contains("Mening buyurt"))
async def my_bookings(message: types.Message):
    lang = db.get_user_language(message.from_user.id)
    bookings = db.get_user_bookings(message.from_user.id)
    if not bookings:
        await message.answer(get_text(lang, 'my_bookings_empty'))
        return
    # Разделим бронирования по статусам
    active_bookings = [b for b in bookings if b[3] in ['pending', 'confirmed']]
    completed_bookings = [b for b in bookings if b[3] == 'completed']
    cancelled_bookings = [b for b in bookings if b[3] == 'cancelled']
    total_text = f"📋 <b>Мои бронирования</b>\n\n"
    total_text += f"🟢 Активные: {len(active_bookings)}\n"
    total_text += f"✅ Завершенные: {len(completed_bookings)}\n"
    total_text += f"❌ Отмененные: {len(cancelled_bookings)}"
    await message.answer(total_text, parse_mode="HTML", reply_markup=booking_filters_keyboard(lang, len(active_bookings), len(completed_bookings), len(cancelled_bookings)))

@dp.callback_query(lambda c: c.data in ["bookings_active", "bookings_completed", "bookings_cancelled"])
async def filter_bookings(callback: types.CallbackQuery):
    lang = db.get_user_language(callback.from_user.id)
    bookings = db.get_user_bookings(callback.from_user.id)
    active_bookings = [b for b in bookings if b[3] in ['pending', 'confirmed']]
    completed_bookings = [b for b in bookings if b[3] == 'completed']
    cancelled_bookings = [b for b in bookings if b[3] == 'cancelled']
    status_map = {
        "bookings_active": (active_bookings, "🟢 Активные"),
        "bookings_completed": (completed_bookings, "✅ Завершенные"),
        "bookings_cancelled": (cancelled_bookings, "❌ Отмененные")
    }
    selected, label = status_map.get(callback.data, ([], ""))
    if not selected:
        await callback.answer("Нет бронирований", show_alert=True)
        return
    await callback.message.edit_text(
        f"{label}: {len(selected)}",
        reply_markup=booking_filters_keyboard(lang, len(active_bookings), len(completed_bookings), len(cancelled_bookings)),
        parse_mode="HTML"
    )
    for booking in selected:
        try:
            quantity = int(booking[6]) if len(booking) > 6 and booking[6] is not None else 1
            discount_price = float(booking[9]) if len(booking) > 9 and booking[9] is not None else 0.0
            total_price = int(discount_price * quantity)
            status_emoji = {"pending": "⏳", "confirmed": "✅", "completed": "🎉", "cancelled": "❌"}
            status_text = {"pending": "Ожидает", "confirmed": "Подтвержден", "completed": "Завершен", "cancelled": "Отменен"}
            text = f"🎫 <b>#{booking[0]}</b> {status_emoji.get(booking[3], '📋')} {status_text.get(booking[3], booking[3])}\n"
            text += f"🍽 {booking[8]}\n"  # title
            text += f"🏪 {booking[11]}\n"  # store_name
            text += f"📦 Количество: {quantity} шт\n"
            text += f"💰 {total_price:,} сум\n"
            text += f"📍 {booking[13]}, {booking[12]}\n"  # city, address
            text += f"🕐 {booking[10]}\n\n"  # available_until
            text += f"🎫 Код: <code>{booking[4]}</code>"
            keyboard = InlineKeyboardBuilder()
            if booking[3] == 'pending':
                keyboard.button(text="❌ Отменить", callback_data=f"cancel_booking_{booking[0]}")
                keyboard.button(text="✅ Завершить", callback_data=f"complete_booking_{booking[0]}")
                keyboard.adjust(2)
            elif booking[3] == 'confirmed':
                keyboard.button(text="✅ Завершить", callback_data=f"complete_booking_{booking[0]}")
                keyboard.button(text="⭐ Оценить", callback_data=f"rate_booking_{booking[0]}")
                keyboard.adjust(2)
            await callback.message.answer(text, parse_mode="HTML", reply_markup=keyboard.as_markup())
        except Exception as e:
            logger.error(f"Error displaying booking {booking[0]}: {e}")
            continue

@dp.callback_query(F.data.startswith("cancel_booking_"))
async def cancel_booking(callback: types.CallbackQuery):
    lang = db.get_user_language(callback.from_user.id)
    booking_id = int(callback.data.split("_")[2])
    
    booking = db.get_booking(booking_id)
    if booking and booking[3] in ['pending', 'confirmed']:  # Можно отменить pending и confirmed
        offer = db.get_offer(booking[1])
        if offer:
            db.cancel_booking(booking_id)
            # Возвращаем забронированное количество в остаток
            qty = int(booking[6]) if len(booking) > 6 and booking[6] is not None else 1
            db.increment_offer_quantity(booking[1], qty)
            try:
                METRICS["bookings_cancelled"] += 1
            except Exception:
                pass
        
        # Обновляем сообщение
        await callback.message.edit_text(
            callback.message.text + f"\n\n❌ {get_text(lang, 'booking_cancelled')}"
        )
        
        # Отправляем уведомление покупателю
        customer_id = booking[2]  # user_id из booking
        customer_lang = db.get_user_language(customer_id)
        
        if offer:
            store = db.get_store(offer[1])
            
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            customer_kb = InlineKeyboardBuilder()
            customer_kb.button(text="🏠 Главное меню", callback_data="main_menu")
            
            try:
                await bot.send_message(
                    customer_id,
                    f"❌ <b>Бронирование отменено</b>\n\n"
                    f"🎫 Бронь #{booking_id}\n"
                    f"🏪 {store[2] if store else 'Магазин'}\n"
                    f"🍽 {offer[2]}\n\n"
                    f"Извините за неудобства. Товар снова доступен для бронирования.",
                    parse_mode="HTML",
                    reply_markup=customer_kb.as_markup()
                )
            except Exception as e:
                logger.error(f"Failed to notify customer {customer_id}: {e}")
        
    await callback.answer()

@dp.callback_query(F.data == "main_menu")
async def handle_main_menu(callback: types.CallbackQuery):
    """Обработчик кнопки возврата в главное меню"""
    user = db.get_user(callback.from_user.id)
    if not user:
        await callback.answer(get_text(lang, "user_not_found"))
        return
    
    lang = db.get_user_language(callback.from_user.id)
    menu = main_menu_seller(lang) if user[6] == "seller" else main_menu_customer(lang)
    
    await callback.message.answer(
        get_text(lang, 'welcome_back', name=callback.from_user.first_name, city=user[4]),
        parse_mode="HTML",
        reply_markup=menu
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("complete_booking_"))
async def complete_booking(callback: types.CallbackQuery):
    """Завершает бронирование и уведомляет покупателя"""
    lang = db.get_user_language(callback.from_user.id)
    booking_id = int(callback.data.split("_")[2])
    
    booking = db.get_booking(booking_id)
    if booking and booking[3] in ['pending', 'confirmed']:
        # Обновляем статус на completed
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE bookings SET status = ? WHERE booking_id = ?', ('completed', booking_id))
        conn.commit()
        conn.close()
        
        # Обновляем сообщение партнёра
        await callback.message.edit_text(
            callback.message.text + f"\n\n✅ Заказ завершен! Спасибо за покупку!"
        )
        
        # Отправляем уведомление покупателю с кнопкой оценки
        customer_id = booking[2]  # user_id из booking
        customer_lang = db.get_user_language(customer_id)
        
        # Получаем информацию о товаре и магазине
        offer = db.get_offer(booking[1])
        if offer:
            store = db.get_store(offer[1])
            
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            customer_kb = InlineKeyboardBuilder()
            customer_kb.button(text="⭐ Оценить магазин", callback_data=f"rate_booking_{booking_id}")
            
            try:
                await bot.send_message(
                    customer_id,
                    f"✅ <b>Заказ выдан!</b>\n\n"
                    f"🎫 Бронь #{booking_id}\n"
                    f"🏪 {store[2] if store else 'Магазин'}\n"
                    f"🍽 {offer[2]}\n\n"
                    f"Спасибо за покупку! Оцените ваш опыт:",
                    parse_mode="HTML",
                    reply_markup=customer_kb.as_markup()
                )
            except Exception as e:
                logger.error(f"Failed to notify customer {customer_id}: {e}")
        
    await callback.answer()

@dp.callback_query(F.data.startswith("rate_booking_"))
async def rate_booking(callback: types.CallbackQuery):
    """Оценить магазин после завершения заказа"""
    lang = db.get_user_language(callback.from_user.id)
    booking_id = int(callback.data.split("_")[2])
    
    booking = db.get_booking(booking_id)
    if not booking:
        await callback.answer("❌ Бронирование не найдено", show_alert=True)
        return
    
    # Получаем информацию о магазине через предложение
    offer = db.get_offer(booking[1])
    if not offer:
        await callback.answer("❌ Предложение не найдено", show_alert=True)
        return
    
    store_id = offer[1]
    store = db.get_store(store_id)
    
    # Создаем клавиатуру с рейтингами
    keyboard = InlineKeyboardBuilder()
    for rating in range(1, 6):
        keyboard.button(text=f"{'⭐' * rating}", callback_data=f"booking_rate_{booking_id}_{rating}")
    keyboard.adjust(5)
    
    text = f"⭐ <b>Оцените ваш заказ</b>\n\n🎫 #{booking_id}\n🏪 {store[2]}\n\nВыберите оценку:"
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("booking_rate_"))
async def save_booking_rating(callback: types.CallbackQuery):
    """Сохраняет оценку заказа"""
    lang = db.get_user_language(callback.from_user.id)
    parts = callback.data.split("_")
    booking_id = int(parts[2])
    rating = int(parts[3])
    
    # Получаем информацию о бронировании и магазине
    booking = db.get_booking(booking_id)
    offer = db.get_offer(booking[1])
    store_id = offer[1]
    
    # Сохраняем рейтинг
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO ratings (booking_id, user_id, store_id, rating) VALUES (?, ?, ?, ?)', 
                  (booking_id, callback.from_user.id, store_id, rating))
    conn.commit()
    conn.close()
    
    await callback.message.edit_text(
        f"✅ Спасибо за оценку: {'⭐' * rating}\n\nВаш отзыв поможет другим покупателям!"
    )
    await callback.answer()

# ============== СТАТЬ ПАРТНЁРОМ ==============

@dp.message(F.text.contains("Стать партнером") | F.text.contains("Hamkor bolish"))
async def become_partner(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    user = db.get_user(message.from_user.id)
    
    # ПРОВЕРКА: если пользователя нет в БД
    if not user:
        await message.answer(
            get_text(lang, 'choose_language'),
            reply_markup=language_keyboard()
        )
        return
    
    # Проверяем: если уже партнер И есть магазин - просто переключаем режим
    # user: [0]user_id, [1]username, [2]first_name, [3]phone, [4]city, [5]language, [6]role, [7]is_admin, [8]notifications
    if user[6] == 'seller':
        # Проверяем, есть ли у партнера ОДОБРЕННЫЙ магазин
        if has_approved_store(message.from_user.id):
            # Remember seller view preference
            user_view_mode[message.from_user.id] = 'seller'
            await message.answer(
                get_text(lang, 'switched_to_seller'),
                reply_markup=main_menu_seller(lang)
            )
            return
        else:
            # Если нет одобренного магазина - показываем статус
            stores = db.get_user_stores(message.from_user.id)
            if stores:
                # Есть магазин(ы), но не одобрены
                status = stores[0][8] if len(stores[0]) > 8 else 'pending'
                if status == 'pending':
                    await message.answer(
                        get_text(lang, 'no_approved_stores'),
                        reply_markup=main_menu_customer(lang)
                    )
                elif status == 'rejected':
                    # Можно подать заявку заново
                    await message.answer(
                        get_text(lang, 'store_rejected') + "\n\nПодайте заявку заново:",
                        reply_markup=main_menu_customer(lang)
                    )
                    # Продолжаем регистрацию новой заявки
                else:
                    await message.answer(
                        get_text(lang, 'no_approved_stores'),
                        reply_markup=main_menu_customer(lang)
                    )
                return
    
    # Если не партнер или нет магазина - начинаем регистрацию
    await message.answer(
        get_text(lang, 'become_partner_text'),
        parse_mode="HTML",
        reply_markup=city_keyboard(lang)
    )
    await state.set_state(RegisterStore.city)

@dp.message(RegisterStore.city)
async def register_store_city(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    cities = get_cities(lang)
    city_text = message.text.replace("📍 ", "").strip()
    
    if city_text in cities:
        # КРИТИЧЕСКИ ВАЖНО: Нормализуем город в русское название для единообразия в БД!
        normalized_city = normalize_city(city_text)
        await state.update_data(city=normalized_city)
        await message.answer(
            get_text(lang, 'store_category'),
            reply_markup=category_keyboard(lang)
        )
        await state.set_state(RegisterStore.category)

@dp.message(RegisterStore.category)
async def register_store_category(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    categories = get_categories(lang)
    cat_text = message.text.replace("🏷 ", "").strip()
    
    if cat_text in categories:
        # КРИТИЧЕСКИ ВАЖНО: Нормализуем категорию в русское название для единообразия в БД!
        normalized_category = normalize_category(cat_text)
        await state.update_data(category=normalized_category)
        await message.answer(get_text(lang, 'store_name'), reply_markup=cancel_keyboard(lang))
        await state.set_state(RegisterStore.name)

@dp.message(RegisterStore.name)
async def register_store_name(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(name=message.text)
    await message.answer(get_text(lang, 'store_address'))
    await state.set_state(RegisterStore.address)

@dp.message(RegisterStore.address)
async def register_store_address(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    logger.info(f"Handler register_store_address called, user {message.from_user.id}, address: {message.text}")
    await state.update_data(address=message.text)
    description_text = get_text(lang, 'store_description')
    logger.info(f"Sending description prompt: {description_text}")
    await message.answer(description_text, reply_markup=cancel_keyboard(lang))
    await state.set_state(RegisterStore.description)

@dp.message(RegisterStore.description)
async def register_store_description(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(description=message.text)
    data = await state.get_data()
    
    # Используем телефон пользователя из профиля
    user = db.get_user(message.from_user.id)
    owner_phone = user[3] if user and len(user) > 3 else None
    
    # Создаём заявку на магазин (статус pending)
    store_id = db.add_store(
        message.from_user.id,
        data['name'],
        data['city'],
        data['address'],
        data['description'],
        data['category'],
        owner_phone
    )
    
    await state.clear()
    
    # Сообщение пользователю о модерации
    await message.answer(
        get_text(
            lang,
            'store_pending',
            name=data['name'], city=data['city'], address=data['address'],
            category=data['category'], description=data['description'], phone=owner_phone or '—'
        ),
        parse_mode="HTML",
        reply_markup=main_menu_customer(lang)
    )

    # (Опционально) Можно уведомить админа о новой заявке тут
    # try:
    #     await bot.send_message(ADMIN_ID, f"🆕 Новая заявка на партнёрство: {data['name']} ({data['city']})")
    # except Exception:
    #     pass
    
    # Уведомляем ВСЕХ админов
    admins = db.get_all_admins()
    for admin in admins:
        try:
            admin_text = (
                f"🔔 <b>Новая заявка на партнерство!</b>\n\n"
                f"От: {message.from_user.full_name} (@{message.from_user.username or 'нет'})\n"
                f"ID: <code>{message.from_user.id}</code>\n\n"
                f"🏪 {data['name']}\n"
                f"📍 {data['city']}, {data['address']}\n"
                f"🏷 {data['category']}\n"
                f"📝 {data['description']}\n"
                f"📱 {message.text}\n\n"
                f"Перейдите в админ панель для модерации."
            )
            await bot.send_message(admin[0], admin_text, parse_mode="HTML")
        except Exception:
            pass

# ============== СОЗДАНИЕ ПРЕДЛОЖЕНИЯ (УПРОЩЁННАЯ ВЕРСИЯ 3 ШАГА) ==============

@dp.message(F.text.contains("Добавить") | F.text.contains("Qo'shish"))
async def add_offer_start(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    
    # ВАЖНО: Берём только ОДОБРЕННЫЕ магазины!
    stores = db.get_approved_stores(message.from_user.id)
    
    if not stores:
        await message.answer(get_text(lang, 'no_approved_stores'))
        return
    
    if len(stores) == 1:
        # Один магазин - сразу начинаем создание
        await state.update_data(store_id=stores[0][0])
        
        # Клавиатура с кнопкой "Без фото"
        builder = InlineKeyboardBuilder()
        builder.button(text="📝 Без фото" if lang == 'ru' else "📝 Fotosiz", callback_data="create_no_photo")
        builder.adjust(1)
        
        step1_text = (
            f"🏪 <b>{stores[0][2]}</b>\n\n"
            f"<b>{'ШАГ 1 из 3: НАЗВАНИЕ И ФОТО' if lang == 'ru' else '1-QADAM 3 tadan: NOM VA RASM'}</b>\n\n"
            f"📝 {'Введите название товара' if lang == 'ru' else 'Mahsulot nomini kiriting'}\n\n"
            f"� {'Можете сразу отправить фото с названием в подписи или нажать кнопку' if lang == 'ru' else 'Rasmni nom bilan yuboring yoki tugmani bosing'}"
        )
        
        await message.answer(
            step1_text,
            parse_mode="HTML",
            reply_markup=builder.as_markup()
        )
        await state.set_state(CreateOffer.title)
    else:
        # Несколько магазинов - нужно выбрать
        await message.answer(
            get_text(lang, 'choose_store'),
            reply_markup=cancel_keyboard(lang)
        )
        text = ""
        for i, store in enumerate(stores, 1):
            text += f"{i}. 🏪 {store[2]} - 📍 {store[3]}\n"
        await message.answer(text)
        await state.set_state(CreateOffer.store_id)

@dp.message(CreateOffer.store_id)
async def create_offer_store_selected(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    stores = db.get_approved_stores(message.from_user.id)
    
    try:
        store_num = int(message.text)
        if 1 <= store_num <= len(stores):
            selected_store = stores[store_num - 1]
            await state.update_data(store_id=selected_store[0])
            
            # Клавиатура с кнопкой "Без фото"
            builder = InlineKeyboardBuilder()
            builder.button(text="📝 Без фото" if lang == 'ru' else "📝 Fotosiz", callback_data="create_no_photo")
            builder.adjust(1)
            
            step1_text = (
                f"🏪 <b>{selected_store[2]}</b>\n\n"
                f"<b>{'ШАГ 1 из 3' if lang == 'ru' else '1-QADAM 3 tadan'}</b>\n\n"
                f"📝 {'Введите название товара' if lang == 'ru' else 'Mahsulot nomini kiriting'}\n"
                f"📸 Затем отправьте фото (или нажмите кнопку 'Без фото')" if lang == 'ru'
                else f"📸 Keyin rasmni yuboring (yoki 'Fotosiz' tugmasini bosing)"
            )
            
            await message.answer(
                step1_text,
                parse_mode="HTML",
                reply_markup=builder.as_markup()
            )
            await state.set_state(CreateOffer.title)
        else:
            await message.answer(get_text(lang, 'error_invalid_number'))
    except Exception:
        await message.answer(get_text(lang, 'error_invalid_number'))

@dp.message(CreateOffer.title, F.photo)
async def create_offer_title_with_photo(message: types.Message, state: FSMContext):
    """Если пользователь сразу отправил фото с названием в caption"""
    lang = db.get_user_language(message.from_user.id)
    title = message.caption if message.caption else "Товар"
    photo_id = message.photo[-1].file_id
    
    await state.update_data(title=title, photo=photo_id)
    
    # ШАГ 2: Цены и количество
    builder = InlineKeyboardBuilder()
    # Популярные варианты скидок
    builder.button(text="30%", callback_data="discount_30")
    builder.button(text="40%", callback_data="discount_40")
    builder.button(text="50%", callback_data="discount_50")
    builder.button(text="60%", callback_data="discount_60")
    builder.adjust(4)
    
    await message.answer(
        f"<b>{'ШАГ 2 из 3: ЦЕНЫ И КОЛИЧЕСТВО' if lang == 'ru' else '2-QADAM 3 tadan: NARXLAR VA MIQDOR'}</b>\n\n"
        f"💡 {'Быстрый формат' if lang == 'ru' else 'Tez format'}:\n"
        f"<code>{'обычная_цена скидка% количество' if lang == 'ru' else 'oddiy_narx chegirma% miqdor'}</code>\n\n"
        f"📝 {'Пример' if lang == 'ru' else 'Misol'}: <code>1000 40% 50</code>\n"
        f"   {'(обычная цена 1000, скидка 40%, количество 50)' if lang == 'ru' else '(oddiy narx 1000, chegirma 40%, miqdor 50)'}\n\n"
        f"{'Или введите только обычную цену и выберите % скидки кнопкой ⬇️' if lang == 'ru' else 'Yoki faqat oddiy narxni kiriting va tugma bilan % chegirmani tanlang ⬇️'}",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await state.set_state(CreateOffer.original_price)

@dp.message(CreateOffer.title)
async def create_offer_title(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(title=message.text)
    
    # Теперь спрашиваем про фото
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Без фото" if lang == 'ru' else "📝 Fotosiz", callback_data="create_skip_photo")
    builder.adjust(1)
    
    await message.answer(
        f"✅ {'Название' if lang == 'ru' else 'Nom'}: <b>{message.text}</b>\n\n"
        f"📸 {'Теперь отправьте фото товара или нажмите кнопку' if lang == 'ru' else 'Endi mahsulot rasmini yuboring yoki tugmani bosing'}",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await state.set_state(CreateOffer.photo)
@dp.callback_query(F.data == "create_no_photo")
async def offer_without_photo(callback: types.CallbackQuery, state: FSMContext):
    """Создание без фото сразу после начала"""
    lang = db.get_user_language(callback.from_user.id)
    await callback.message.edit_text(
        f"<b>{'ШАГ 1 из 3' if lang == 'ru' else '1-QADAM 3 tadan'}</b>\n\n"
        f"📝 {'Введите название товара' if lang == 'ru' else 'Mahsulot nomini kiriting'}:",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "create_skip_photo")
async def skip_photo_goto_step2(callback: types.CallbackQuery, state: FSMContext):
    """Пропустить фото и перейти к шагу 2"""
    lang = db.get_user_language(callback.from_user.id)
    
    # Сохраняем что фото нет
    await state.update_data(photo=None)
    
    # ШАГ 2: Цены и количество
    builder = InlineKeyboardBuilder()
    builder.button(text="30%", callback_data="discount_30")
    builder.button(text="40%", callback_data="discount_40")
    builder.button(text="50%", callback_data="discount_50")
    builder.button(text="60%", callback_data="discount_60")
    builder.adjust(4)
    
    await callback.message.edit_text(
        f"<b>{'ШАГ 2 из 3: ЦЕНЫ И КОЛИЧЕСТВО' if lang == 'ru' else '2-QADAM 3 tadan: NARXLAR VA MIQDOR'}</b>\n\n"
        f"{'Введите в формате' if lang == 'ru' else 'Formatda kiriting'}:\n"
        f"<code>{'обычная_цена скидка количество' if lang == 'ru' else 'oddiy_narx chegirma miqdor'}</code>\n\n"
        f"{'Пример' if lang == 'ru' else 'Misol'}: <code>1000 40% 50</code>\n"
        f"{'(цена 1000, скидка 40%, количество 50 шт)' if lang == 'ru' else '(narx 1000, chegirma 40%, miqdor 50 dona)'}\n\n"
        f"{'Или просто введите обычную цену и выберите % скидки:' if lang == 'ru' else 'Yoki oddiy narxni kiriting va chegirma % tanlang:'}",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await state.set_state(CreateOffer.original_price)
    await callback.answer()

@dp.message(CreateOffer.photo, F.photo)
async def create_offer_photo_received(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    photo_id = message.photo[-1].file_id
    await state.update_data(photo=photo_id)
    
    # ШАГ 2: Цены и количество
    builder = InlineKeyboardBuilder()
    builder.button(text="30%", callback_data="discount_30")
    builder.button(text="40%", callback_data="discount_40")
    builder.button(text="50%", callback_data="discount_50")
    builder.button(text="60%", callback_data="discount_60")
    builder.adjust(4)
    
    await message.answer(
        f"<b>{'ШАГ 2 из 3: ЦЕНЫ И КОЛИЧЕСТВО' if lang == 'ru' else '2-QADAM 3 tadan: NARXLAR VA MIQDOR'}</b>\n\n"
        f"{'Введите в формате' if lang == 'ru' else 'Formatda kiriting'}:\n"
        f"<code>{'обычная_цена скидка количество' if lang == 'ru' else 'oddiy_narx chegirma miqdor'}</code>\n\n"
        f"{'Пример' if lang == 'ru' else 'Misol'}: <code>1000 40% 50</code>\n"
        f"{'(цена 1000, скидка 40%, количество 50 шт)' if lang == 'ru' else '(narx 1000, chegirma 40%, miqdor 50 dona)'}\n\n"
        f"{'Или просто введите обычную цену и выберите % скидки:' if lang == 'ru' else 'Yoki oddiy narxni kiriting va chegirma % tanlang:'}",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await state.set_state(CreateOffer.original_price)

# Обработчики кнопок процента скидки
@dp.callback_query(F.data.startswith("discount_"))
async def select_discount_percent(callback: types.CallbackQuery, state: FSMContext):
    """Пользователь выбрал процент скидки кнопкой"""
    lang = db.get_user_language(callback.from_user.id)
    percent = int(callback.data.split("_")[1])
    
    await state.update_data(discount_percent=percent)
    await callback.message.edit_text(
        f"✅ {'Скидка' if lang == 'ru' else 'Chegirma'}: <b>{percent}%</b>\n\n"
        f"{'Теперь введите обычную цену и количество:' if lang == 'ru' else 'Endi oddiy narx va miqdorni kiriting:'}\n"
        f"{'Формат' if lang == 'ru' else 'Format'}: <code>{'цена количество' if lang == 'ru' else 'narx miqdor'}</code>\n"
        f"{'Пример' if lang == 'ru' else 'Misol'}: <code>1000 50</code>",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.message(CreateOffer.original_price)
async def create_offer_prices_and_quantity(message: types.Message, state: FSMContext):
    """Упрощённый обработчик: принимает цену скидку и количество одной строкой"""
    lang = db.get_user_language(message.from_user.id)
    
    try:
        parts = message.text.split()
        data = await state.get_data()
        
        # Проверяем, выбрал ли пользователь скидку кнопкой
        if 'discount_percent' in data:
            # Формат: "цена количество"
            if len(parts) == 2:
                original_price = float(parts[0])
                quantity = int(parts[1])
                discount_percent = data['discount_percent']
                discount_price = original_price * (1 - discount_percent / 100)
            else:
                await message.answer("❌ Неверный формат. Введите: цена количество\nПример: 1000 50")
                return
        else:
            # Формат: "цена скидка количество"
            if len(parts) == 3:
                original_price = float(parts[0])
                discount_str = parts[1].replace('%', '')
                discount_percent = float(discount_str)
                quantity = int(parts[2])
                discount_price = original_price * (1 - discount_percent / 100)
            elif len(parts) == 2:
                # Если только цена и количество без скидки - спрашиваем
                await message.answer(
                    f"{'Введите процент скидки или выберите кнопкой:' if lang == 'ru' else 'Chegirma foizini kiriting yoki tugmani tanlang:'}",
                    reply_markup=InlineKeyboardBuilder()
                    .button(text="30%", callback_data="discount_30")
                    .button(text="40%", callback_data="discount_40")
                    .button(text="50%", callback_data="discount_50")
                    .button(text="60%", callback_data="discount_60")
                    .adjust(4)
                    .as_markup()
                )
                await state.update_data(original_price=float(parts[0]), quantity=int(parts[1]))
                return
            else:
                await message.answer(
                    "❌ Неверный формат.\n"
                    "Введите: цена скидка% количество\n"
                    "Пример: 1000 40% 50"
                )
                return
        
        # Проверки
        if original_price <= 0 or discount_price <= 0 or quantity <= 0:
            await message.answer("❌ Все значения должны быть больше 0")
            return
        
        if discount_price >= original_price:
            await message.answer("❌ Цена со скидкой должна быть меньше обычной")
            return
        
        # Сохраняем
        await state.update_data(
            original_price=original_price,
            discount_price=discount_price,
            quantity=quantity,
            unit='шт',  # по умолчанию штуки
            description=data.get('title', 'Описание не указано')  # используем название как описание
        )
        
        # ШАГ 3: Категория товара (сначала выбираем категорию)
        builder = InlineKeyboardBuilder()
        # Только категории, без срока годности
        builder.button(text="🍞 Выпечка", callback_data="prodcat_bakery")
        builder.button(text="🥛 Молочка", callback_data="prodcat_dairy")
        builder.button(text="🥩 Мясо", callback_data="prodcat_meat")
        builder.button(text="🍎 Фрукты", callback_data="prodcat_fruits")
        builder.button(text="🥬 Овощи", callback_data="prodcat_vegetables")
        builder.button(text="🎯 Другое", callback_data="prodcat_other")
        builder.adjust(3, 3)  # 2 ряда по 3 кнопки
        
        uz_note = "(Kategoriyani tanlagandan keyin yaroqlilik muddatini ko'rsatasiz)"
        
        await message.answer(
            f"<b>{'ШАГ 3 из 3: КАТЕГОРИЯ' if lang == 'ru' else '3-QADAM 3 tadan: KATEGORIYA'}</b>\n\n"
            f"{'Выберите категорию товара:' if lang == 'ru' else 'Mahsulot kategoriyasini tanlang:'}\n\n"
            f"{'(После выбора категории укажете срок годности)' if lang == 'ru' else uz_note}",
            parse_mode="HTML",
            reply_markup=builder.as_markup()
        )
        await state.set_state(CreateOffer.category)  # Используем категорию как последнее состояние
        
    except ValueError:
        await message.answer(
            "❌ Ошибка в формате чисел.\n"
            "Пример: 1000 40% 50"
        )
    except Exception as e:
        logger.error(f"Error in create_offer_prices_and_quantity: {e}")
        await message.answer("❌ Ошибка обработки. Попробуйте снова.")

# Обработчики выбора категории и срока
@dp.callback_query(F.data.startswith("prodcat_"), CreateOffer.category)
async def select_category_simple(callback: types.CallbackQuery, state: FSMContext):
    """Выбор категории товара - ОБЯЗАТЕЛЬНО сначала категория"""
    lang = db.get_user_language(callback.from_user.id)
    category_key = callback.data.split("_")[1]
    
    await state.update_data(category=category_key)
    
    # После выбора категории показываем ТОЛЬКО кнопки срока годности
    from datetime import datetime, timedelta
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    
    builder = InlineKeyboardBuilder()
    builder.button(text=f"Сегодня {today.strftime('%d.%m')}", callback_data=f"exp_today")
    builder.button(text=f"Завтра {tomorrow.strftime('%d.%m')}", callback_data=f"exp_tomorrow")
    builder.button(text="Неделя", callback_data="exp_week")
    builder.adjust(3)
    
    # Показываем название выбранной категории
    category_names = {
        'bakery': '🍞 Выпечка',
        'dairy': '� Молочка',
        'meat': '🥩 Мясо',
        'fruits': '� Фрукты',
        'vegetables': '🥬 Овощи',
        'other': '🎯 Другое'
    }
    
    await callback.message.edit_text(
        f"<b>{'ШАГ 3 из 3: СРОК ГОДНОСТИ' if lang == 'ru' else '3-QADAM 3 tadan: YAROQLILIK MUDDATI'}</b>\n\n"
        f"✅ {'Категория:' if lang == 'ru' else 'Kategoriya:'} {category_names.get(category_key, '🎯 Другое')}\n\n"
        f"{'Выберите срок годности:' if lang == 'ru' else 'Yaroqlilik muddatini tanlang:'}",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("exp_"), CreateOffer.category)
async def select_expiry_simple(callback: types.CallbackQuery, state: FSMContext):
    """Выбор срока годности и создание товара"""
    lang = db.get_user_language(callback.from_user.id)
    exp_key = callback.data.split("_")[1]
    
    from datetime import datetime, timedelta
    today = datetime.now()
    
    # ВАЖНО: Устанавливаем срок годности на КОНЕЦ дня (23:59:59)
    # чтобы товары не удалялись в середине дня
    if exp_key == "today":
        # Сегодня до конца дня
        expiry_date = today.strftime('%Y-%m-%d')
    elif exp_key == "tomorrow":
        # Завтра до конца дня
        expiry_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    elif exp_key == "week":
        # Через неделю
        expiry_date = (today + timedelta(days=7)).strftime('%Y-%m-%d')
    else:
        expiry_date = today.strftime('%Y-%m-%d')
    
    data = await state.get_data()
    
    # Проверяем что категория выбрана
    if 'category' not in data:
        await callback.answer("❌ Ошибка: категория не выбрана", show_alert=True)
        return
    
    # Создаём предложение
    logger.info(f"Creating offer: store_id={data.get('store_id')}, title={data.get('title')}, category={data.get('category')}")
    
    offer_id = db.add_offer(
        data['store_id'],
        data['title'],
        data.get('description', data['title']),
        data['original_price'],
        data['discount_price'],
        data['quantity'],
        "18:00",  # available_from
        "21:00",  # available_until
        data.get('photo'),
        expiry_date,
        data.get('unit', 'шт'),
        data.get('category', 'other')
    )
    
    logger.info(f"Offer created with ID: {offer_id}")
    
    await state.clear()
    
    discount_percent = int((1 - data['discount_price'] / data['original_price']) * 100)
    
    # Название категории для отображения
    category_names = {
        'bakery': '🍞 Выпечка',
        'dairy': '🥛 Молочка',
        'meat': '🥩 Мясо',
        'fruits': '🍎 Фрукты',
        'vegetables': '🥬 Овощи',
        'other': '🎯 Другое'
    }
    category_display = category_names.get(data.get('category', 'other'), '🎯 Другое')
    
    await callback.message.edit_text(
        f"✅ <b>{'ТОВАР СОЗДАН!' if lang == 'ru' else 'MAHSULOT YARATILDI!'}</b>\n\n"
        f"📦 {data['title']}\n"
        f"🏷️ {category_display}\n"
        f"💰 {int(data['original_price'])} ➜ {int(data['discount_price'])} сум (-{discount_percent}%)\n"
        f"📊 {data['quantity']} шт\n"
        f"📅 До: {expiry_date}\n"
        f"⏰ Забор: 18:00-21:00",
        parse_mode="HTML"
    )
    
    await callback.message.answer(
        f"{'Что дальше?' if lang == 'ru' else 'Keyingi qadam?'}",
        reply_markup=main_menu_seller(lang)
    )
    
    await callback.answer("✅ Готово!" if lang == 'ru' else "✅ Tayyor!")

# СТАРЫЕ ОБРАБОТЧИКИ (закомментированы, так как используется упрощённый flow)
# Оставлены для справки и возможного восстановления

# @dp.message(CreateOffer.photo)
# async def create_offer_no_photo(message: types.Message, state: FSMContext):
#     lang = db.get_user_language(message.from_user.id)
#     await state.update_data(photo=None)
#     await message.answer(get_text(lang, 'original_price'))
#     await state.set_state(CreateOffer.original_price)

# @dp.callback_query(F.data == "skip_photo")
# async def skip_photo(callback: types.CallbackQuery, state: FSMContext):
#     lang = db.get_user_language(callback.from_user.id)
#     await state.update_data(photo=None)
#     await callback.message.edit_text(get_text(lang, 'original_price'))
#     await state.set_state(CreateOffer.original_price)
#     await callback.answer()

@dp.message(CreateOffer.photo)
async def create_offer_photo_fallback(message: types.Message, state: FSMContext):
    """Fallback если пользователь отправил текст вместо фото"""
    lang = db.get_user_language(message.from_user.id)
    
    # Считаем это как пропуск фото и переходим к шагу 2
    builder = InlineKeyboardBuilder()
    builder.button(text="30%", callback_data="discount_30")
    builder.button(text="40%", callback_data="discount_40")
    builder.button(text="50%", callback_data="discount_50")
    builder.button(text="60%", callback_data="discount_60")
    builder.adjust(4)
    
    await message.answer(
        f"<b>{'ШАГ 2 из 3: ЦЕНЫ И КОЛИЧЕСТВО' if lang == 'ru' else '2-QADAM 3 tadan: NARXLAR VA MIQDOR'}</b>\n\n"
        f"{'Введите в формате' if lang == 'ru' else 'Formatda kiriting'}:\n"
        f"<code>{'обычная_цена скидка количество' if lang == 'ru' else 'oddiy_narx chegirma miqdor'}</code>\n\n"
        f"{'Пример' if lang == 'ru' else 'Misol'}: <code>1000 40% 50</code>\n"
        f"{'(цена 1000, скидка 40%, количество 50 шт)' if lang == 'ru' else '(narx 1000, chegirma 40%, miqdor 50 dona)'}\n\n"
        f"{'Или просто введите обычную цену и выберите % скидки:' if lang == 'ru' else 'Yoki oddiy narxni kiriting va chegirma % tanlang:'}",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await state.set_state(CreateOffer.original_price)

# Старые обработчики для совместимости (не используются в новом упрощённом flow)
# Закомментированы, так как весь процесс теперь занимает 3 шага вместо 12

# СТАРЫЙ ОБРАБОТЧИК - НЕ ИСПОЛЬЗУЕТСЯ
'''
@dp.message(CreateOffer.original_price)
async def create_offer_original_price(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    try:
        price = float(message.text)
        if price <= 0:
            await message.answer("❌ Цена должна быть больше 0")
            return
        if price > 100000000:  # 100 миллионов - разумный лимит
            await message.answer("❌ Слишком большая цена")
            return
        await state.update_data(original_price=price)
        await message.answer(get_text(lang, 'discount_price'))
        await state.set_state(CreateOffer.discount_price)
    except ValueError:
        await message.answer(get_text(lang, 'error_invalid_number'))
    except Exception as e:
        logger.error(f"Error in create_offer_original_price: {e}")
        await message.answer("❌ Ошибка обработки. Попробуйте снова.")

@dp.message(CreateOffer.discount_price)
async def create_offer_discount_price(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    try:
        price = float(message.text)
        if price <= 0:
            await message.answer("❌ Цена должна быть больше 0")
            return
        
        data = await state.get_data()
        original_price = data.get('original_price', 0)
        
        if price >= original_price:
            await message.answer("❌ Цена со скидкой должна быть меньше обычной цены")
            return
        
        discount_percent = int((1 - price / original_price) * 100)
        if discount_percent < 10:
            await message.answer("⚠️ Внимание: скидка меньше 10%. Рекомендуем делать скидку от 30% для привлечения клиентов.")
        
        await state.update_data(discount_price=price)
        await message.answer(get_text(lang, 'quantity'))
        await state.set_state(CreateOffer.quantity)
    except ValueError:
        await message.answer(get_text(lang, 'error_invalid_number'))
    except Exception as e:
        logger.error(f"Error in create_offer_discount_price: {e}")
        await message.answer("❌ Ошибка обработки. Попробуйте снова.")

@dp.message(CreateOffer.quantity)
async def create_offer_quantity(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    try:
        qty = int(message.text)
        if qty <= 0:
            await message.answer("❌ Количество должно быть больше 0")
            return
        if qty > 10000:
            await message.answer("❌ Слишком большое количество (максимум 10000)")
            return
        await state.update_data(quantity=qty)
        await message.answer("📏 Выберите единицы измерения:", reply_markup=units_keyboard(lang))
        await state.set_state(CreateOffer.unit)
    except ValueError:
        await message.answer(get_text(lang, 'error_invalid_number'))
    except Exception as e:
        logger.error(f"Error in create_offer_quantity: {e}")
        await message.answer("❌ Ошибка обработки. Попробуйте снова.")

@dp.message(CreateOffer.unit)
async def create_offer_unit(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(unit=message.text)
    await message.answer("🏷 Выберите категорию товара:", reply_markup=product_categories_keyboard(lang))
    await state.set_state(CreateOffer.category)

@dp.message(CreateOffer.category)
async def create_offer_category(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    # Извлекаем английское название категории из эмодзи текста
    category_map = {
        '🍞 Хлеб и выпечка': 'bakery', '🥛 Молочные продукты': 'dairy', '🥩 Мясо и птица': 'meat',
        '🐟 Рыба и морепродукты': 'fish', '🥬 Овощи': 'vegetables', '🍎 Фрукты и ягоды': 'fruits',
        '🧀 Сыры': 'cheese', '🥚 Яйца': 'eggs', '🍚 Крупы и макароны': 'grains',
        '🥫 Консервы': 'canned', '🍫 Кондитерские изделия': 'sweets', '🍪 Печенье и снэки': 'snacks',
        '☕ Чай и кофе': 'drinks_hot', '🥤 Напитки': 'drinks', '🧴 Бытовая химия': 'household',
        '🧼 Гигиена': 'hygiene', '🏠 Для дома': 'home', '🎯 Другое': 'other'
    }
    category = category_map.get(message.text, 'other')
    await state.update_data(category=category)
    await message.answer(get_text(lang, 'time_from'), reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(CreateOffer.available_from)

@dp.message(CreateOffer.available_from)
async def create_offer_time_from(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    
    # Валидация формата времени HH:MM
    import re
    time_pattern = r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$'
    if not re.match(time_pattern, message.text.strip()):
        error_msg = "❌ Неверный формат времени! Введите в формате ЧЧ:ММ (например: 18:00)" if lang == 'ru' else "❌ Noto'g'ri vaqt formati! ЧЧ:ММ formatida kiriting (masalan: 18:00)"
        await message.answer(error_msg)
        return
    
    await state.update_data(available_from=message.text.strip())
    
    # Клавиатура для срока годности
    builder = InlineKeyboardBuilder()
    from datetime import datetime, timedelta
    today = datetime.now()
    builder.button(text=f"Сегодня ({today.strftime('%d.%m.%Y')})", callback_data=f"expiry_{today.strftime('%d.%m.%Y')}")
    builder.button(text=f"Завтра ({(today + timedelta(days=1)).strftime('%d.%m.%Y')})", callback_data=f"expiry_{(today + timedelta(days=1)).strftime('%d.%m.%Y')}")
    builder.button(text=f"Неделя ({(today + timedelta(days=7)).strftime('%d.%m.%Y')})", callback_data=f"expiry_{(today + timedelta(days=7)).strftime('%d.%m.%Y')}")
    builder.button(text="Ввести вручную", callback_data="expiry_manual")
    builder.adjust(1)
    
    await message.answer(get_text(lang, 'expiry_date'), reply_markup=builder.as_markup())
    await state.set_state(CreateOffer.expiry_date)

@dp.message(CreateOffer.expiry_date)
async def create_offer_expiry_date(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    
    # Конвертируем дату из dd.mm.yyyy в yyyy-mm-dd
    date_str = message.text.strip()
    try:
        if '.' in date_str:
            day, month, year = date_str.split('.')
            formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            formatted_date = date_str  # Если уже в правильном формате
        await state.update_data(expiry_date=formatted_date)
    except Exception:
        await state.update_data(expiry_date=date_str)  # Сохраняем как есть, если не удалось
    
    await message.answer(get_text(lang, 'time_until'))
    await state.set_state(CreateOffer.available_until)

@dp.message(CreateOffer.available_until)
async def create_offer_time_until(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    
    # Валидация формата времени HH:MM
    import re
    time_pattern = r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$'
    if not re.match(time_pattern, message.text.strip()):
        error_msg = "❌ Неверный формат времени! Введите в формате ЧЧ:ММ (например: 21:00)" if lang == 'ru' else "❌ Noto'g'ri vaqt formati! ЧЧ:ММ formatida kiriting (masalan: 21:00)"
        await message.answer(error_msg)
        return
    
    data = await state.get_data()
    
    # Теперь expiry_date и available_until - это отдельные поля
    # expiry_date - срок годности продукта (например "31.12.2025")
    # available_from и available_until - время когда можно забрать (например "18:00" - "21:00")
    
    offer_id = db.add_offer(
        data['store_id'],
        data['title'],
        data['description'],
        data['original_price'],
        data['discount_price'],
        data['quantity'],
        data['available_from'],  # Время начала (например "18:00")
        message.text,  # Время окончания (например "21:00")
        data.get('photo'),
        data.get('expiry_date'),  # Срок годности (например "31.12.2025")
        data.get('unit', 'шт'),   # Единицы измерения
        data.get('category', 'other')  # Категория товара
    )
    
    await state.clear()
    
    discount = int((1 - data['discount_price'] / data['original_price']) * 100)
    unit = data.get('unit', 'шт')
    text = get_text(lang, 'offer_created',
                   title=data['title'],
                   description=data['description'],
                   original_price=f"{int(data['original_price']):,}",
                   discount_price=f"{int(data['discount_price']):,}",
                   discount=discount,
                   quantity=f"{data['quantity']} {unit}",
                   time_from=data['available_from'],
                   time_until=message.text)
    
    # Добавляем информацию о сроке годности и категории
    if data.get('expiry_date'):
        text += f"\n\n📅 Срок годности: {data['expiry_date']}"
    if data.get('category') and data['category'] != 'other':
        category_names = {
            'bakery': 'Хлеб и выпечка', 'dairy': 'Молочные продукты', 'meat': 'Мясо и птица',
            'fish': 'Рыба и морепродукты', 'vegetables': 'Овощи', 'fruits': 'Фрукты и ягоды',
            'cheese': 'Сыры', 'eggs': 'Яйца', 'grains': 'Крупы и макароны', 'canned': 'Консервы',
            'sweets': 'Кондитерские изделия', 'snacks': 'Печенье и снэки', 'drinks_hot': 'Чай и кофе',
            'drinks': 'Напитки', 'household': 'Бытовая химия', 'hygiene': 'Гигиена', 'home': 'Для дома'
        }
        category_name = category_names.get(data['category'], data['category'])
        text += f"\n🏷 Категория: {category_name}"
    
    text += f"\n🕐 Время забора: {data['available_from']} - {message.text}"
    
    if data.get('photo'):
        await message.answer_photo(
            photo=data['photo'],
            caption=text,
            parse_mode="HTML",
            reply_markup=main_menu_seller(lang)
        )
    else:
        await message.answer(text, parse_mode="HTML", reply_markup=main_menu_seller(lang))

@dp.callback_query(F.data.startswith("expiry_"))
async def select_expiry_date(callback: types.CallbackQuery, state: FSMContext):
    lang = db.get_user_language(callback.from_user.id)
    if callback.data == "expiry_manual":
        await callback.message.edit_text("Введите срок годности в формате дд.мм.гггг:")
        return
    
    date_str = callback.data.split("_", 1)[1]  # expiry_dd.mm.yyyy
    # Конвертируем
    try:
        day, month, year = date_str.split('.')
        formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        await state.update_data(expiry_date=formatted_date)
    except Exception:
        await state.update_data(expiry_date=date_str)
    
    # Клавиатура для времени окончания
    builder = InlineKeyboardBuilder()
    builder.button(text="18:00", callback_data="time_18:00")
    builder.button(text="20:00", callback_data="time_20:00")
    builder.button(text="22:00", callback_data="time_22:00")
    builder.button(text="Ввести вручную", callback_data="time_manual")
    builder.adjust(2)
    
    await callback.message.edit_text(get_text(lang, 'time_until'), reply_markup=builder.as_markup())
    await state.set_state(CreateOffer.available_until)
    await callback.answer()

@dp.callback_query(F.data.startswith("time_"))
async def select_time_until(callback: types.CallbackQuery, state: FSMContext):
    lang = db.get_user_language(callback.from_user.id)
    if callback.data == "time_manual":
        await callback.message.edit_text("Введите время окончания в формате чч:мм:")
        return
    
    time_str = callback.data.split("_", 1)[1]
    await state.update_data(available_until=time_str)
    
    # Теперь создаем предложение
    data = await state.get_data()
    
    # Проверяем наличие всех необходимых данных
    if not all(key in data for key in ['store_id', 'title', 'description', 'original_price', 'discount_price', 'quantity', 'available_from']):
        await callback.answer("Ошибка: данные потеряны. Начните добавление товара заново.", show_alert=True)
        await state.clear()
        return
    
    offer_id = db.add_offer(
        data['store_id'],
        data['title'],
        data['description'],
        data['original_price'],
        data['discount_price'],
        data['quantity'],
        data['available_from'],
        time_str,
        data.get('photo'),
        data.get('expiry_date'),
        data.get('unit', 'шт'),
        data.get('category', 'other')
    )
    
    await state.clear()
    
    discount = int((1 - data['discount_price'] / data['original_price']) * 100)
    unit = data.get('unit', 'шт')
    text = get_text(lang, 'offer_created',
                   title=data['title'],
                   description=data['description'],
                   original_price=f"{int(data['original_price']):,} {get_text(lang, 'currency')}",
                   discount_price=f"{int(data['discount_price']):,} {get_text(lang, 'currency')}",
                   discount=discount,
                   quantity=f"{data['quantity']} {unit}",
                   time_from=data['available_from'],
                   time_until=time_str)
    
    if data.get('expiry_date'):
        text += f"\n\n📅 {get_text(lang, 'expires_on')}: {data['expiry_date']}"
    if data.get('category') and data['category'] != 'other':
        category_names = {
            'bakery': 'Хлеб и выпечка', 'dairy': 'Молочные продукты', 'meat': 'Мясо и птица',
            'fish': 'Рыба и морепродукты', 'vegetables': 'Овощи', 'fruits': 'Фрукты и ягоды',
            'cheese': 'Сыры', 'eggs': 'Яйца', 'grains': 'Крупы и макароны', 'canned': 'Консервы',
            'sweets': 'Кондитерские изделия', 'snacks': 'Печенье и снэки', 'drinks_hot': 'Чай и кофе',
            'drinks': 'Напитки', 'household': 'Бытовая химия', 'hygiene': 'Гигиена', 'home': 'Для дома'
        }
        text += f"\n🏷 Категория: {category_names.get(data['category'], data['category'])}"
    
    # ИСПРАВЛЕНИЕ: Отправляем новое сообщение вместо edit_text
    try:
        if data.get('photo'):
            await bot.send_photo(
                chat_id=callback.message.chat.id,
                photo=data['photo'],
                caption=text,
                parse_mode="HTML",
                reply_markup=main_menu_seller(lang)
            )
        else:
            await bot.send_message(
                chat_id=callback.message.chat.id,
                text=text,
                parse_mode="HTML",
                reply_markup=main_menu_seller(lang)
            )
        # Удаляем старое сообщение с кнопками
        await callback.message.delete()
    except Exception as e:
        # Если не удалось отправить с фото, отправляем текст
        await bot.send_message(
            chat_id=callback.message.chat.id,
            text=text,
            parse_mode="HTML",
            reply_markup=main_menu_seller(lang)
        )
        try:
            await callback.message.delete()
        except Exception:
            pass
    
    await callback.answer()
'''
# КОНЕЦ СТАРЫХ ОБРАБОТЧИКОВ - ВСЕ ВЫШЕПЕРЕЧИСЛЕННЫЕ ФУНКЦИИ ЗАКОММЕНТИРОВАНЫ
# Они заменены на упрощённый 3-шаговый процесс

# ============== МАССОВОЕ СОЗДАНИЕ ==============

@dp.message(F.text.contains("Массовое создание") | F.text.contains("Ommaviy yaratish"))
async def bulk_create_start(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    stores = db.get_approved_stores(message.from_user.id)
    
    if not stores:
        await message.answer(get_text(lang, 'no_approved_stores'))
        return
    
    if len(stores) == 1:
        # Один магазин - сразу начинаем
        await state.update_data(store_id=stores[0][0])
        await message.answer(
            get_text(lang, 'bulk_create_start', store_name=stores[0][2]),
            parse_mode="HTML",
            reply_markup=cancel_keyboard(lang)
        )
        await state.set_state(BulkCreate.title)
    else:
        # Несколько магазинов - нужно выбрать
        await message.answer(
            get_text(lang, 'choose_store'),
            reply_markup=cancel_keyboard(lang)
        )
        text = ""
        for i, store in enumerate(stores, 1):
            text += f"{i}. 🏪 {store[2]} - 📍 {store[3]}\n"
        await message.answer(text)
        await state.set_state(BulkCreate.store_id)

@dp.message(BulkCreate.store_id)
async def bulk_create_store_selected(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    stores = db.get_approved_stores(message.from_user.id)
    
    try:
        store_num = int(message.text)
        if 1 <= store_num <= len(stores):
            selected_store = stores[store_num - 1]
            await state.update_data(store_id=selected_store[0])
            await message.answer(
                get_text(lang, 'bulk_create_start', store_name=selected_store[2]),
                parse_mode="HTML",
                reply_markup=cancel_keyboard(lang)
            )
            await state.set_state(BulkCreate.title)
        else:
            await message.answer(get_text(lang, 'error_invalid_number'))
    except Exception:
        await message.answer(get_text(lang, 'error_invalid_number'))

@dp.message(BulkCreate.title)
async def bulk_create_title(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(title=message.text)
    await message.answer(get_text(lang, 'offer_description'))
    await state.set_state(BulkCreate.description)

@dp.message(BulkCreate.description)
async def bulk_create_description(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(description=message.text)
    await message.answer(
        get_text(lang, 'send_photo'),
        reply_markup=cancel_keyboard(lang)
    )
    await state.set_state(BulkCreate.photo)

@dp.message(BulkCreate.photo, F.photo)
async def bulk_create_photo(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(photo=message.photo[-1].file_id)
    await message.answer(get_text(lang, 'original_price'))
    await state.set_state(BulkCreate.original_price)

@dp.message(BulkCreate.photo)
async def bulk_create_no_photo(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(photo=None)
    await message.answer(get_text(lang, 'original_price'))
    await state.set_state(BulkCreate.original_price)

@dp.message(BulkCreate.original_price)
async def bulk_create_original_price(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    try:
        await state.update_data(original_price=float(message.text))
        await message.answer(get_text(lang, 'discount_price'))
        await state.set_state(BulkCreate.discount_price)
    except Exception:
        await message.answer(get_text(lang, 'error_invalid_number'))

@dp.message(BulkCreate.discount_price)
async def bulk_create_discount_price(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    try:
        await state.update_data(discount_price=float(message.text))
        await message.answer(get_text(lang, 'quantity'))
        await state.set_state(BulkCreate.quantity)
    except Exception:
        await message.answer(get_text(lang, 'error_invalid_number'))

@dp.message(BulkCreate.quantity)
async def bulk_create_quantity(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    try:
        await state.update_data(quantity=int(message.text))
        await message.answer(get_text(lang, 'time_from'))
        await state.set_state(BulkCreate.available_from)
    except Exception:
        await message.answer(get_text(lang, 'error_invalid_number'))

@dp.message(BulkCreate.available_from)
async def bulk_create_time_from(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(available_from=message.text)
    await message.answer(get_text(lang, 'time_until'))
    await state.set_state(BulkCreate.available_until)

@dp.message(BulkCreate.available_until)
async def bulk_create_time_until(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await state.update_data(available_until=message.text)
    await message.answer(get_text(lang, 'bulk_count'), parse_mode="HTML")
    await state.set_state(BulkCreate.count)

@dp.message(BulkCreate.count)
async def bulk_create_count(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    try:
        count = int(message.text)
        if count < 1 or count > 100:
            await message.answer(get_text(lang, 'invalid_range'))
            return
        
        data = await state.get_data()
        created = 0
        
        for i in range(count):
            offer_id = db.add_offer(
                data['store_id'],
                data['title'],
                data['description'],
                data['original_price'],
                data['discount_price'],
                data['quantity'],
                data['available_from'],
                data['available_until'],
                data.get('photo')
            )
            if offer_id:
                created += 1
        
        await state.clear()
        
        discount = int((1 - data['discount_price'] / data['original_price']) * 100)
        total_qty = data['quantity'] * created
        
        text = get_text(lang, 'bulk_created',
                       count=created,
                       title=data['title'],
                       description=data['description'],
                       original_price=f"{int(data['original_price']):,}",
                       discount_price=f"{int(data['discount_price']):,}",
                       discount=discount,
                       quantity=data['quantity'],
                       total_quantity=total_qty,
                       time_from=data['available_from'],
                       time_until=data['available_until'])
        
        await message.answer(text, parse_mode="HTML", reply_markup=main_menu_seller(lang))
    except Exception:
        await message.answer(get_text(lang, 'error_invalid_number'))

# ============== МОИ ПРЕДЛОЖЕНИЯ ==============

@dp.message(F.text.contains("Мои товары") | F.text.contains("Мои предложения") | F.text.contains("Mening mahsulotlarim") | F.text.contains("Mening taklif"))
async def my_offers(message: types.Message):
    lang = db.get_user_language(message.from_user.id)
    stores = db.get_user_stores(message.from_user.id)
    
    logger.info(f"my_offers: user {message.from_user.id}, stores count: {len(stores)}")
    
    if not stores:
        await message.answer(get_text(lang, 'no_stores'))
        return
    
    all_offers = []
    for store in stores:
        offers = db.get_store_offers(store[0])
        logger.info(f"Store {store[0]} ({store[2]}), offers count: {len(offers)}")
        all_offers.extend(offers)
    
    logger.info(f"Total offers: {len(all_offers)}")
    
    if not all_offers:
        await message.answer(get_text(lang, 'no_offers_yet'))
        return
    
    await message.answer(
        f"📦 <b>{'Ваши товары' if lang == 'ru' else 'Sizning mahsulotlaringiz'}</b>\n"
        f"{'Найдено' if lang == 'ru' else 'Topildi'}: {len(all_offers)} {'товаров' if lang == 'ru' else 'mahsulot'}",
        parse_mode="HTML"
    )
    
    # Отправляем каждый товар отдельным сообщением с inline-кнопками
    for offer in all_offers[:20]:  # Показываем до 20 товаров
        offer_id = offer[0]
        title = offer[2]
        original_price = int(offer[4])
        discount_price = int(offer[5])
        quantity = offer[6]
        status = offer[9]
        
        # Определяем единицы измерения
        if len(offer) >= 14 and offer[13]:
            unit = offer[13]
        else:
            unit = 'шт'
        
        discount_percent = int((1 - discount_price / original_price) * 100) if original_price > 0 else 0
        
        # Формируем текст
        status_emoji = '✅' if status == 'active' else '❌'
        text = f"{status_emoji} <b>{title}</b>\n\n"
        text += f"💰 {original_price:,} ➜ <b>{discount_price:,}</b> сум (-{discount_percent}%)\n"
        text += f"📦 {'Осталось' if lang == 'ru' else 'Qoldi'}: <b>{quantity}</b> {unit}\n"
        
        # Срок годности (expiry_date на позиции 9)
        if len(offer) > 9 and offer[9]:
            expiry_info = db.get_time_remaining(offer[9])
            if expiry_info:
                text += f"{expiry_info}\n"
            else:
                text += f"📅 До: {offer[9]}\n"
        
        # Время забора
        text += f"🕐 {offer[7]} - {offer[8]}"
        
        # Создаём inline-кнопки для быстрых действий
        builder = InlineKeyboardBuilder()
        
        if status == 'active':
            # Кнопки для активного товара
            builder.button(text="➕ +1", callback_data=f"qty_add_{offer_id}")
            builder.button(text="➖ -1", callback_data=f"qty_sub_{offer_id}")
            builder.button(text="📝 Изменить" if lang == 'ru' else "📝 Tahrirlash", callback_data=f"edit_offer_{offer_id}")
            builder.button(text="🔄 Продлить" if lang == 'ru' else "🔄 Uzaytirish", callback_data=f"extend_offer_{offer_id}")
            builder.button(text="❌ Снять" if lang == 'ru' else "❌ O'chirish", callback_data=f"deactivate_offer_{offer_id}")
            builder.adjust(2, 2, 1)
        else:
            # Кнопки для неактивного товара
            builder.button(text="✅ Активировать" if lang == 'ru' else "✅ Faollashtirish", callback_data=f"activate_offer_{offer_id}")
            builder.button(text="🗑 Удалить" if lang == 'ru' else "🗑 O'chirish", callback_data=f"delete_offer_{offer_id}")
            builder.adjust(2)
        
        # Отправляем сообщение с фото или без
        if offer[10]:  # Если есть фото
            try:
                await message.answer_photo(
                    photo=offer[10],
                    caption=text,
                    parse_mode="HTML",
                    reply_markup=builder.as_markup()
                )
            except Exception:
                # Если фото не загрузилось, отправляем текстом
                await message.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())
        else:
            await message.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())
        
        await asyncio.sleep(0.1)  # Небольшая задержка между сообщениями
    
    if len(all_offers) > 20:
        await message.answer(
            f"... {'и ещё' if lang == 'ru' else 'va yana'} {len(all_offers) - 20} {'товаров' if lang == 'ru' else 'mahsulot'}"
        )

# ============== БЫСТРЫЕ ДЕЙСТВИЯ С ТОВАРАМИ ==============

@dp.callback_query(F.data.startswith("qty_add_"))
async def quantity_add(callback: types.CallbackQuery):
    """Увеличить количество товара на 1"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    # Увеличиваем количество
    new_quantity = offer[6] + 1
    db.update_offer_quantity(offer_id, new_quantity)
    
    # Обновляем сообщение
    await update_offer_message(callback, offer_id, lang)
    await callback.answer(f"✅ +1 (теперь {new_quantity})")

@dp.callback_query(F.data.startswith("qty_sub_"))
async def quantity_subtract(callback: types.CallbackQuery):
    """Уменьшить количество товара на 1"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    # Уменьшаем количество (минимум 0)
    new_quantity = max(0, offer[6] - 1)
    db.update_offer_quantity(offer_id, new_quantity)
    
    # Обновляем сообщение
    await update_offer_message(callback, offer_id, lang)
    
    if new_quantity == 0:
        await callback.answer("⚠️ Количество 0 - товар снят с продажи", show_alert=True)
    else:
        await callback.answer(f"✅ -1 (теперь {new_quantity})")

@dp.callback_query(F.data.startswith("extend_offer_"))
async def extend_offer(callback: types.CallbackQuery):
    """Продлить срок годности товара"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    # Показываем кнопки для выбора нового срока
    from datetime import datetime, timedelta
    today = datetime.now()
    
    builder = InlineKeyboardBuilder()
    builder.button(text=f"Сегодня {today.strftime('%d.%m')}", callback_data=f"setexp_{offer_id}_0")
    builder.button(text=f"Завтра {(today + timedelta(days=1)).strftime('%d.%m')}", callback_data=f"setexp_{offer_id}_1")
    builder.button(text=f"+2 дня {(today + timedelta(days=2)).strftime('%d.%m')}", callback_data=f"setexp_{offer_id}_2")
    builder.button(text=f"+3 дня {(today + timedelta(days=3)).strftime('%d.%m')}", callback_data=f"setexp_{offer_id}_3")
    builder.button(text=f"Неделя {(today + timedelta(days=7)).strftime('%d.%m')}", callback_data=f"setexp_{offer_id}_7")
    builder.button(text="❌ Отмена", callback_data=f"cancel_extend")
    builder.adjust(2, 2, 1, 1)
    
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())
    await callback.answer("📅 Выберите новый срок годности")

@dp.callback_query(F.data.startswith("setexp_"))
async def set_expiry(callback: types.CallbackQuery):
    """Установить новый срок годности"""
    lang = db.get_user_language(callback.from_user.id)
    parts = callback.data.split("_")
    offer_id = int(parts[1])
    days_add = int(parts[2])
    
    from datetime import datetime, timedelta
    new_expiry = (datetime.now() + timedelta(days=days_add)).strftime('%Y-%m-%d')
    
    # Обновляем срок
    db.update_offer_expiry(offer_id, new_expiry)
    
    # Обновляем сообщение
    await update_offer_message(callback, offer_id, lang)
    await callback.answer(f"✅ Срок продлён до {new_expiry}")

@dp.callback_query(F.data == "cancel_extend")
async def cancel_extend(callback: types.CallbackQuery):
    """Отмена продления срока"""
    lang = db.get_user_language(callback.from_user.id)
    # Возвращаем исходные кнопки - нужно получить offer_id из сообщения
    await callback.answer("❌ Отменено")
    # Просто закрываем меню
    await callback.message.edit_reply_markup(reply_markup=None)

@dp.callback_query(F.data.startswith("deactivate_offer_"))
async def deactivate_offer(callback: types.CallbackQuery):
    """Снять товар с продажи"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    # Деактивируем
    db.deactivate_offer(offer_id)
    
    # Обновляем сообщение
    await update_offer_message(callback, offer_id, lang)
    await callback.answer("✅ Товар снят с продажи")

@dp.callback_query(F.data.startswith("activate_offer_"))
async def activate_offer(callback: types.CallbackQuery):
    """Активировать товар"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    # Активируем
    db.activate_offer(offer_id)
    
    # Обновляем сообщение
    await update_offer_message(callback, offer_id, lang)
    await callback.answer("✅ Товар активирован")

@dp.callback_query(F.data.startswith("delete_offer_"))
async def delete_offer(callback: types.CallbackQuery):
    """Удалить товар"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    # Удаляем
    db.delete_offer(offer_id)
    
    # Удаляем сообщение
    await callback.message.delete()
    await callback.answer("🗑 Товар удалён")

@dp.callback_query(F.data.startswith("edit_offer_"))
async def edit_offer(callback: types.CallbackQuery):
    """Показать меню редактирования товара"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    # Меню редактирования
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Изменить цену" if lang == 'ru' else "💰 Narxni o'zgartirish", callback_data=f"edit_price_{offer_id}")
    kb.button(text="📦 Изменить количество" if lang == 'ru' else "📦 Sonini o'zgartirish", callback_data=f"edit_quantity_{offer_id}")
    kb.button(text="� Изменить время" if lang == 'ru' else "🕐 Vaqtni o'zgartirish", callback_data=f"edit_time_{offer_id}")
    kb.button(text="�📝 Изменить описание" if lang == 'ru' else "📝 Tavsifni o'zgartirish", callback_data=f"edit_description_{offer_id}")
    kb.button(text="🔙 Назад" if lang == 'ru' else "🔙 Orqaga", callback_data=f"back_to_offer_{offer_id}")
    kb.adjust(1)
    
    try:
        await callback.message.edit_reply_markup(reply_markup=kb.as_markup())
    except Exception:
        await callback.answer(get_text(lang, "edit_unavailable"), show_alert=True)
    
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_time_"))
async def edit_time_start(callback: types.CallbackQuery, state: FSMContext):
    """Начать редактирование времени забора"""
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[2])
    
    offer = db.get_offer(offer_id)
    if not offer:
        await callback.answer(get_text(lang, "offer_not_found"), show_alert=True)
        return
    
    # Проверка владельца
    user_stores = db.get_user_stores(callback.from_user.id)
    if not any(store[0] == offer[1] for store in user_stores):
        await callback.answer(get_text(lang, "not_your_offer"), show_alert=True)
        return
    
    await state.update_data(offer_id=offer_id)
    await state.set_state(EditOffer.available_from)
    
    await callback.message.answer(
        f"🕐 <b>Изменение времени забора</b>\n\n"
        f"Текущее время: {offer[7]} - {offer[8]}\n\n"
        f"{'Введите новое время начала (например: 18:00):' if lang == 'ru' else 'Yangi boshlanish vaqtini kiriting (masalan: 18:00):'}",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.message(EditOffer.available_from)
async def edit_time_from(message: types.Message, state: FSMContext):
    """Обработка времени начала"""
    lang = db.get_user_language(message.from_user.id)
    
    # Валидация формата времени
    import re
    time_pattern = r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$'
    if not re.match(time_pattern, message.text.strip()):
        error_msg = "❌ Неверный формат! Введите время в формате ЧЧ:ММ (например: 18:00)" if lang == 'ru' else "❌ Noto'g'ri format! ЧЧ:ММ formatida vaqt kiriting (masalan: 18:00)"
        await message.answer(error_msg)
        return
    
    await state.update_data(available_from=message.text.strip())
    await message.answer(
        f"{'Введите время окончания (например: 21:00):' if lang == 'ru' else 'Tugash vaqtini kiriting (masalan: 21:00):'}",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(EditOffer.available_until)

@dp.message(EditOffer.available_until)
async def edit_time_until(message: types.Message, state: FSMContext):
    """Завершение редактирования времени"""
    lang = db.get_user_language(message.from_user.id)
    
    # Валидация формата времени
    import re
    time_pattern = r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$'
    if not re.match(time_pattern, message.text.strip()):
        error_msg = "❌ Неверный формат! Введите время в формате ЧЧ:ММ (например: 21:00)" if lang == 'ru' else "❌ Noto'g'ri format! ЧЧ:ММ formatida vaqt kiriting (masalan: 21:00)"
        await message.answer(error_msg)
        return
    
    data = await state.get_data()
    offer_id = data['offer_id']
    available_from = data['available_from']
    available_until = message.text.strip()
    
    # Обновление в БД
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE offers SET available_from = ?, available_until = ? WHERE offer_id = ?',
        (available_from, available_until, offer_id)
    )
    conn.commit()
    conn.close()
    
    await message.answer(
        f"✅ {'Время забора обновлено!' if lang == 'ru' else 'Olib ketish vaqti yangilandi!'}\n\n"
        f"🕐 {available_from} - {available_until}",
        reply_markup=main_menu_seller(lang)
    )
    await state.clear()

async def update_offer_message(callback: types.CallbackQuery, offer_id: int, lang: str):
    """Обновить сообщение с товаром"""
    offer = db.get_offer(offer_id)
    if not offer:
        return
    
    title = offer[2]
    original_price = int(offer[4])
    discount_price = int(offer[5])
    quantity = offer[6]
    status = offer[9]
    
    # Определяем единицы
    if len(offer) >= 14 and offer[13]:
        unit = offer[13]
    else:
        unit = 'шт'
    
    discount_percent = int((1 - discount_price / original_price) * 100) if original_price > 0 else 0
    
    # Формируем текст
    status_emoji = '✅' if status == 'active' else '❌'
    text = f"{status_emoji} <b>{title}</b>\n\n"
    text += f"💰 {original_price:,} ➜ <b>{discount_price:,}</b> сум (-{discount_percent}%)\n"
    text += f"📦 {'Осталось' if lang == 'ru' else 'Qoldi'}: <b>{quantity}</b> {unit}\n"
    
    # Срок годности
    if len(offer) > 12 and offer[12]:
        expiry_info = db.get_time_remaining(offer[12])
        if expiry_info:
            text += f"{expiry_info}\n"
        else:
            text += f"📅 До: {offer[12]}\n"
    
    # Время
    text += f"🕐 {offer[7]} - {offer[8]}"
    
    # Кнопки
    builder = InlineKeyboardBuilder()
    
    if status == 'active':
        builder.button(text="➕ +1", callback_data=f"qty_add_{offer_id}")
        builder.button(text="➖ -1", callback_data=f"qty_sub_{offer_id}")
        builder.button(text="📝 Изменить" if lang == 'ru' else "📝 Tahrirlash", callback_data=f"edit_offer_{offer_id}")
        builder.button(text="🔄 Продлить" if lang == 'ru' else "🔄 Uzaytirish", callback_data=f"extend_offer_{offer_id}")
        builder.button(text="❌ Снять" if lang == 'ru' else "❌ O'chirish", callback_data=f"deactivate_offer_{offer_id}")
        builder.adjust(2, 2, 1)
    else:
        builder.button(text="✅ Активировать" if lang == 'ru' else "✅ Faollashtirish", callback_data=f"activate_offer_{offer_id}")
        builder.button(text="🗑 Удалить" if lang == 'ru' else "🗑 O'chirish", callback_data=f"delete_offer_{offer_id}")
        builder.adjust(2)
    
    # Обновляем сообщение
    try:
        await callback.message.edit_caption(caption=text, parse_mode="HTML", reply_markup=builder.as_markup())
    except Exception:
        try:
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=builder.as_markup())
        except Exception:
            pass

# ============== ДУБЛИРОВАНИЕ/УДАЛЕНИЕ ==============

@dp.callback_query(F.data.startswith("duplicate_"))
async def duplicate_offer(callback: types.CallbackQuery):
    lang = db.get_user_language(callback.from_user.id)
    offer_id = int(callback.data.split("_")[1])
    offer = db.get_offer(offer_id)
    
    if offer:
        # offer из get_offer с JOIN:
        # [0]offer_id, [1]store_id, [2]title, [3]description, [4]original_price, [5]discount_price,
        # [6]quantity, [7]available_from, [8]available_until, [9]expiry_date, [10]status, [11]photo, 
        # [12]created_at, [13]unit (если есть), [14]category (если есть)
        # [15]store_name, [16]address, [17]city, [18]category (из stores после JOIN)
        
        # Для add_offer нужно только базовые поля offers (без JOIN полей)
        unit_val = offer[13] if len(offer) > 13 and isinstance(offer[13], str) and len(offer[13]) <= 5 else 'шт'
        category_val = offer[14] if len(offer) > 14 and offer[14] else 'other'
        
        new_id = db.add_offer(
            store_id=offer[1],
            title=offer[2],
            description=offer[3],
            original_price=offer[4],
            discount_price=offer[5],
            quantity=offer[6],
            available_from=offer[7],
            available_until=offer[8],
            photo=offer[11],  # photo на позиции [11]
            expiry_date=offer[9] if len(offer) > 9 else None,
            unit=unit_val,
            category=category_val
        )
        await callback.answer(get_text(lang, 'duplicated'), show_alert=True)

# ============== ПОДТВЕРЖДЕНИЕ ВЫДАЧИ ==============

@dp.message(F.text.contains("Подтвердить выдачу") | F.text.contains("Berishni"))
async def confirm_delivery_start(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    await message.answer(
        get_text(lang, 'confirm_delivery_prompt'),
        parse_mode="HTML",
        reply_markup=cancel_keyboard(lang)
    )
    await state.set_state(ConfirmOrder.booking_code)

@dp.message(ConfirmOrder.booking_code)
async def confirm_delivery_process(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    code = message.text.upper().strip()
    
    booking = db.get_booking_by_code(code)
    
    if not booking:
        await message.answer(get_text(lang, 'booking_not_found'))
        return
    
    db.complete_booking(booking[0])
    offer = db.get_offer(booking[1])
    
    await state.clear()
    await message.answer(
        get_text(lang, 'order_confirmed',
                booking_id=booking[0],
                customer_name=booking[5],
                price=f"{int(offer[5]):,}"),
        parse_mode="HTML",
        reply_markup=main_menu_seller(lang)
    )
    
    # Отправка оценки клиенту
    customer_lang = db.get_user_language(booking[2])
    store = db.get_store(offer[1])
    try:
        await bot.send_message(
            booking[2],
            get_text(customer_lang, 'rate_store', store_name=store[2]),
            parse_mode="HTML",
            reply_markup=rate_keyboard(booking[0])
        )
    except Exception:
        pass

# ============== РЕЙТИНГ ==============

@dp.callback_query(F.data.startswith("rate_store_"))
async def rate_store_direct(callback: types.CallbackQuery):
    """Оценить магазин напрямую"""
    lang = db.get_user_language(callback.from_user.id)
    store_id = int(callback.data.split("_")[2])
    
    # Получаем информацию о магазине
    store = db.get_store(store_id)
    if not store:
        await callback.answer("❌ Магазин не найден", show_alert=True)
        return
    
    # Создаем клавиатуру с рейтингами
    keyboard = InlineKeyboardBuilder()
    for rating in range(1, 6):
        keyboard.button(text=f"{'⭐' * rating}", callback_data=f"store_rating_{store_id}_{rating}")
    keyboard.adjust(5)
    
    text = f"⭐ <b>Оцените магазин</b>\n\n🏪 {store[2]}\n📍 {store[4]}\n\nВыберите оценку:"
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("rate_"))
async def rate_booking(callback: types.CallbackQuery):
    """Оценить бронирование (НЕ магазин напрямую)"""
    lang = db.get_user_language(callback.from_user.id)
    parts = callback.data.split("_")
    booking_id = int(parts[1])
    rating = int(parts[2])
    
    if db.has_rated_booking(booking_id):
        await callback.answer(get_text(lang, 'already_rated'), show_alert=True)
        return
    
    booking = db.get_booking(booking_id)
    offer = db.get_offer(booking[1])
    store_id = offer[1]
    
    db.add_rating(booking_id, callback.from_user.id, store_id, rating)
    
    await callback.message.edit_text(
        callback.message.text + f"\n\n{'⭐' * rating}\n{get_text(lang, 'rating_saved')}",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("store_rating_"))
async def save_store_rating(callback: types.CallbackQuery):
    """Сохраняет оценку магазина"""
    lang = db.get_user_language(callback.from_user.id)
    parts = callback.data.split("_")
    store_id = int(parts[2])
    rating = int(parts[3])
    
    # Проверяем, не оценивал ли уже пользователь этот магазин
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM ratings WHERE store_id = ? AND user_id = ?', (store_id, callback.from_user.id))
    already_rated = cursor.fetchone()[0] > 0
    
    if already_rated:
        # Обновляем существующую оценку
        cursor.execute('UPDATE ratings SET rating = ? WHERE store_id = ? AND user_id = ?', 
                      (rating, store_id, callback.from_user.id))
        message_text = f"✅ Оценка обновлена: {'⭐' * rating}"
    else:
        # Добавляем новую оценку
        cursor.execute('INSERT INTO ratings (store_id, user_id, rating) VALUES (?, ?, ?)', 
                      (store_id, callback.from_user.id, rating))
        message_text = f"✅ Спасибо за оценку: {'⭐' * rating}"
    
    conn.commit()
    conn.close()
    
    await callback.message.edit_text(message_text, parse_mode="HTML")
    await callback.answer()

# ============== МАГАЗИНЫ ==============

@dp.message(F.text.contains("Все магазины") | F.text.contains("Barcha dokonlar"))
async def all_stores(message: types.Message):
    """Выбор категории магазинов с количеством (для клиентов)"""
    lang = db.get_user_language(message.from_user.id)
    user = db.get_user(message.from_user.id)
    
    if not user:
        await message.answer(get_text(lang, 'error'))
        return
    
    city = user[4]  # город пользователя
    search_city = normalize_city(city)
    
    # Получаем количество магазинов по категориям
    counts = db.get_stores_count_by_category(search_city)
    total = sum(counts.values())
    
    if total == 0:
        await message.answer(
            f"😔 В городе {city} пока нет активных магазинов.\n\n"
            "Станьте первым партнером! 🤝",
            parse_mode="HTML"
        )
        return
    
    # Нормализуем ключи словаря для сопоставления с категориями
    normalized_counts = {}
    categories = get_categories(lang)
    
    for category in categories:
        norm_cat = normalize_category(category)
        normalized_counts[category] = counts.get(norm_cat, 0)
    
    await message.answer(
        f"🏪 <b>{'МАГАЗИНЫ' if lang == 'ru' else 'DOKONLAR'}</b>\n"
        f"📍 {city}\n\n"
        f"{'Всего магазинов' if lang == 'ru' else 'Jami dokonlar'}: {total}\n\n"
        f"{'Выберите категорию:' if lang == 'ru' else 'Kategoriyani tanlang:'}",
        parse_mode="HTML",
        reply_markup=stores_category_selection(lang, normalized_counts)
    )

@dp.callback_query(F.data == "stores_top")
async def show_top_stores(callback: types.CallbackQuery):
    """Показать топ магазины по рейтингу"""
    lang = db.get_user_language(callback.from_user.id)
    user = db.get_user(callback.from_user.id)
    
    if not user:
        await callback.answer(get_text(lang, 'error'), show_alert=True)
        return
    
    city = user[4]
    search_city = normalize_city(city)
    
    stores = db.get_top_stores_by_city(search_city, limit=10)
    
    if not stores:
        await callback.answer("😔 Магазинов не найдено", show_alert=True)
        return
    
    await callback.answer()
    
    await callback.message.edit_text(
        f"⭐ <b>{'ТОП МАГАЗИНЫ' if lang == 'ru' else 'TOP DOKONLAR'}</b>\n"
        f"📍 {city}\n\n"
        f"{'Лучшие по рейтингу' if lang == 'ru' else 'Eng yaxshi reytingli'}:",
        parse_mode="HTML"
    )
    
    # Отправляем карточки магазинов
    for store in stores:
        await send_store_card(callback.message, store, lang, from_callback=True)
        await asyncio.sleep(0.1)

@dp.callback_query(F.data.startswith("stores_cat_"))
async def show_stores_by_category(callback: types.CallbackQuery):
    """Показать магазины выбранной категории"""
    lang = db.get_user_language(callback.from_user.id)
    user = db.get_user(callback.from_user.id)
    
    if not user:
        await callback.answer(get_text(lang, 'error'), show_alert=True)
        return
    
    city = user[4]
    search_city = normalize_city(city)
    cat_index = int(callback.data.split("_")[-1])
    categories = get_categories(lang)
    
    if cat_index >= len(categories):
        await callback.answer("Ошибка", show_alert=True)
        return
    
    category = categories[cat_index]
    category_normalized = normalize_store_category(category)  # ИСПРАВЛЕНО: было normalize_category
    
    # Получаем магазины этой категории в городе пользователя
    stores = db.get_stores_by_category(category_normalized, search_city)
    
    if not stores:
        await callback.answer(f"😔 В категории {category} нет магазинов", show_alert=True)
        return
    
    await callback.answer()
    
    await callback.message.edit_text(
        f"🏪 <b>{category.upper()}</b>\n"
        f"📍 {city}\n\n"
        f"{'Найдено' if lang == 'ru' else 'Topildi'}: {len(stores)} {'магазинов' if lang == 'ru' else 'dokon'}",
        parse_mode="HTML"
    )
    
    # Отправляем карточки магазинов (сортируем по рейтингу)
    for store in stores[:15]:
        await send_store_card(callback.message, store, lang, from_callback=True)
        await asyncio.sleep(0.1)

async def send_store_card(message: types.Message, store: tuple, lang: str, from_callback: bool = False):
    """Отправить карточку магазина"""
    # Безопасный доступ к полям tuple
    store_id = store[0] if len(store) > 0 else None
    store_name = store[2] if len(store) > 2 else "Неизвестный магазин"
    city = store[3] if len(store) > 3 else ""
    address = store[4] if len(store) > 4 else ""
    description = store[5] if len(store) > 5 else ""
    category = store[6] if len(store) > 6 else ""
    phone = store[7] if len(store) > 7 else ""
    
    if not store_id:
        return
    
    # Получаем рейтинг
    avg_rating = db.get_store_average_rating(store_id)
    ratings = db.get_store_ratings(store_id)
    ratings_count = len(ratings)
    
    # Формируем текст карточки
    text = f"🏪 <b>{store_name}</b>\n\n"
    if category:
        text += f"🏷 {category}\n"
    if address:
        text += f"📍 {address}\n"
    if description and description.strip():
        text += f"📝 {description}\n"
    if phone and phone.strip():
        text += f"📞 {phone}\n"
    text += f"⭐ Рейтинг: {avg_rating:.1f}/5 ({ratings_count} {'отзывов' if lang == 'ru' else 'sharh'})"
    
    # Кнопки
    builder = InlineKeyboardBuilder()
    builder.button(text="🛍 Товары магазина" if lang == 'ru' else "🛍 Dokon mahsulotlari", 
                   callback_data=f"show_offers_{store_id}")
    builder.button(text="⭐ Оставить отзыв" if lang == 'ru' else "⭐ Sharh qoldirish", 
                   callback_data=f"rate_store_{store_id}")
    builder.adjust(1)
    
    await message.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())


@dp.message(F.text.contains("Мои магазины") | F.text.contains("Mening dokonlarim"))
async def my_stores(message: types.Message):
    lang = db.get_user_language(message.from_user.id)
    stores = db.get_user_stores(message.from_user.id)
    
    if not stores:
        await message.answer(get_text(lang, 'no_stores'))
        return
    
    await message.answer(get_text(lang, 'your_stores', count=len(stores)))
    
    for store in stores:
        stats = db.get_store_sales_stats(store[0])
       
        avg_rating = db.get_store_average_rating(store[0])

        ratings = db.get_store_ratings(store[0])
        
        text = get_text(lang, 'store_stats',
                       name=store[2],
                       category=store[6],
                       city=store[3],
                       address=store[4],
                       description=store[5],
                       rating=f"{avg_rating:.1f}",
                       reviews=len(ratings),
                       sales=stats['total_sales'],
                       revenue=stats['total_revenue'],
                       pending=stats['pending_bookings'])
        
        await message.answer(text, parse_mode="HTML")

# ============== БРОНИРОВАНИЯ МАГАЗИНА ==============

@dp.message(F.text.contains("Заказы") | F.text.contains("Бронирования магазина") | F.text.contains("Buyurtmalar") | F.text.contains("buyurtmalari"))
async def store_bookings(message: types.Message, state: FSMContext):
    """Показать все бронирования для магазинов партнера с фильтрами"""
    lang = db.get_user_language(message.from_user.id)
    
    # Получаем все магазины партнера
    stores = db.get_approved_stores(message.from_user.id)
    
    if not stores:
        await message.answer(get_text(lang, 'no_approved_stores'))
        return
    
    all_bookings = []
    for store in stores:
        bookings = db.get_store_bookings(store[0])
        all_bookings.extend(bookings)
    
    if not all_bookings:
        await message.answer("📋 Пока нет бронирований")
        return
    
    # Статистика по статусам
    pending_count = len([b for b in all_bookings if b[3] == 'pending'])
    confirmed_count = len([b for b in all_bookings if b[3] == 'confirmed'])
    completed_count = len([b for b in all_bookings if b[3] == 'completed'])
    cancelled_count = len([b for b in all_bookings if b[3] == 'cancelled'])
    
    # Показываем статистику и фильтры
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    filter_kb = InlineKeyboardBuilder()
    filter_kb.button(text=f"⏳ Ожидают ({pending_count})", callback_data="bookings_filter_pending")
    filter_kb.button(text=f"✅ Завершённые ({completed_count})", callback_data="bookings_filter_completed")
    filter_kb.button(text=f"❌ Отменённые ({cancelled_count})", callback_data="bookings_filter_cancelled")
    filter_kb.button(text=f"📋 Все ({len(all_bookings)})", callback_data="bookings_filter_all")
    filter_kb.adjust(2, 2)
    
    await message.answer(
        f"📊 <b>Статистика бронирований</b>\n\n"
        f"⏳ Ожидают: {pending_count}\n"
        f"✅ Завершено: {completed_count}\n"
        f"❌ Отменено: {cancelled_count}\n"
        f"📋 Всего: {len(all_bookings)}\n\n"
        f"Выберите фильтр:",
        parse_mode="HTML",
        reply_markup=filter_kb.as_markup()
    )

# Обработчики фильтров бронирований
@dp.callback_query(F.data.startswith("bookings_filter_"))
async def filter_bookings(callback: types.CallbackQuery):
    """Фильтрация бронирований по статусу"""
    lang = db.get_user_language(callback.from_user.id)
    filter_type = callback.data.split("_")[2]  # pending, completed, cancelled, all
    
    # Получаем все магазины партнера
    stores = db.get_approved_stores(callback.from_user.id)
    
    if not stores:
        await callback.answer(get_text(lang, 'no_approved_stores'), show_alert=True)
        return
    
    all_bookings = []
    for store in stores:
        bookings = db.get_store_bookings(store[0])
        all_bookings.extend(bookings)
    
    # Фильтруем по статусу
    if filter_type == "all":
        filtered_bookings = all_bookings
        filter_name = "Все бронирования"
    else:
        filtered_bookings = [b for b in all_bookings if b[3] == filter_type]
        filter_names = {
            'pending': '⏳ Ожидающие',
            'confirmed': '✔️ Подтверждённые',
            'completed': '✅ Завершённые',
            'cancelled': '❌ Отменённые'
        }
        filter_name = filter_names.get(filter_type, filter_type)
    
    if not filtered_bookings:
        await callback.answer(f"Нет бронирований со статусом: {filter_name}", show_alert=True)
        return
    
    await callback.message.answer(
        f"📋 <b>{filter_name}: {len(filtered_bookings)}</b>",
        parse_mode="HTML"
    )
    
    # Показываем каждое бронирование
    # SQL из get_store_bookings: b.* (8 полей: 0-7), o.title (8), u.first_name (9), u.username (10), u.phone (11)
    # b.* = booking_id[0], offer_id[1], user_id[2], status[3], booking_code[4], pickup_time[5], quantity[6], created_at[7]
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    
    for booking in filtered_bookings[:15]:  # Показываем до 15 бронирований
        # Безопасная конвертация quantity с проверкой типа
        try:
            quantity = int(booking[6]) if len(booking) > 6 and booking[6] is not None else 1
        except (ValueError, TypeError):
            quantity = 1
        customer_phone = booking[11] if len(booking) > 11 and booking[11] else "Не указан"
        
        # Эмодзи статуса
        status_emoji = {
            'pending': '⏳',
            'confirmed': '✔️',
            'completed': '✅',
            'cancelled': '❌'
        }.get(booking[3], '❓')
        
        text = f"{status_emoji} <b>Бронь #{booking[0]}</b>\n\n"
        text += f"🍽 {booking[8]}\n"  # offer title
        text += f"📦 Количество: {quantity} шт\n"
        text += f"👤 {booking[9]}"  # customer name
        if booking[10]:
            text += f" (@{booking[10]})"
        text += f"\n📱 Телефон: <code>{customer_phone}</code>"
        text += f"\n🎫 Код: <code>{booking[4]}</code>"  # booking code
        text += f"\n📅 {booking[7]}"  # created_at
        text += f"\n📊 Статус: {status_emoji} {booking[3]}"
        
        kb = InlineKeyboardBuilder()
        
        # Кнопки только для активных бронирований (pending)
        if booking[3] == 'pending':
            kb.button(text="✅ Выдано", callback_data=f"complete_booking_{booking[0]}")
            kb.button(text="❌ Отменить", callback_data=f"cancel_booking_{booking[0]}")
            kb.adjust(2)
            
            await callback.message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())
        else:
            # Для завершённых/отменённых только информация
            await callback.message.answer(text, parse_mode="HTML")
    
    await callback.answer()

# ============== СМЕНА ГОРОДА ==============

@dp.message(F.text.contains("Мой город") | F.text.contains("Mening shahrim"))
async def show_my_city(message: types.Message, state: FSMContext):
    """Показать текущий город пользователя и предложить смену города"""
    user_id = message.from_user.id
    lang = db.get_user_language(user_id)
    user = db.get_user(user_id)
    current_city = user[4] if user and len(user) > 4 else None
    if not current_city:
        current_city = "Не выбран"
    text = f"🌆 {get_text(lang, 'your_city') if 'your_city' in globals() else 'Ваш город'}: {current_city}\n\n{get_text(lang, 'change_city_prompt') if 'change_city_prompt' in globals() else 'Хотите изменить город?'}"
    await message.answer(
        text,
        reply_markup=city_keyboard(lang)
    )
    await state.set_state(ChangeCity.city)

@dp.message(ChangeCity.city)
@secure_user_input
async def change_city_process(message: types.Message, state: FSMContext):
    lang = db.get_user_language(message.from_user.id)
    cities = get_cities(lang)
    city_text = message.text.replace("📍 ", "").strip()
    
    # Validate city input
    if not validator.validate_city(city_text):
        await message.answer(get_text(lang, 'invalid_city'))
        return
    
    if city_text in cities:
        db.update_user_city(message.from_user.id, city_text)
        await state.clear()
        user = db.get_user(message.from_user.id)
        menu = main_menu_seller(lang) if user[6] == "seller" else main_menu_customer(lang)
        await message.answer(
            get_text(lang, 'city_changed', city=city_text),
            reply_markup=menu
        )

# ============== ИЗБРАННОЕ ==============

@dp.message(F.text.contains("Избранное") | F.text.contains("Sevimlilar"))
async def show_favorites(message: types.Message):
    """Показать избранные магазины"""
    lang = db.get_user_language(message.from_user.id)
    user_id = message.from_user.id
    
    # Получаем избранные магазины
    favorites = db.get_favorites(user_id)
    
    if not favorites:
        await message.answer(get_text(lang, 'no_favorites'))
        return
    
    await message.answer(f"❤️ <b>Ваши избранные магазины ({len(favorites)})</b>", parse_mode="HTML")
    
    for store in favorites:
        store_id = store[0]
        avg_rating = db.get_store_average_rating(store_id)
        ratings = db.get_store_ratings(store_id)
        
        text = f"""🏪 <b>{store[2]}</b>
🏷 {store[6]}
📍 {store[4]}
📝 {store[5]}
⭐ Рейтинг: {avg_rating:.1f}/5 ({len(ratings)} отзывов)"""
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🛍 Товары магазина", callback_data=f"show_offers_{store_id}")
        keyboard.button(text="💔 Удалить из избранного", callback_data=f"unfavorite_{store_id}")
        keyboard.adjust(1)
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard.as_markup())

@dp.callback_query(F.data.startswith("favorite_"))
async def toggle_favorite(callback: types.CallbackQuery):
    """Добавить магазин в избранное"""
    store_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    lang = db.get_user_language(user_id)
    
    # Проверяем, уже в избранном или нет
    if db.is_favorite(user_id, store_id):
        await callback.answer(get_text(lang, 'already_in_favorites'), show_alert=True)
    else:
        db.add_favorite(user_id, store_id)
        await callback.answer(get_text(lang, 'added_to_favorites'), show_alert=True)

@dp.callback_query(F.data.startswith("unfavorite_"))
async def remove_favorite(callback: types.CallbackQuery):
    """Удалить магазин из избранного"""
    store_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    lang = db.get_user_language(user_id)
    
    db.remove_favorite(user_id, store_id)
    await callback.message.delete()
    await callback.answer(get_text(lang, 'removed_from_favorites'), show_alert=True)

# ============== АНАЛИТИКА ДЛЯ ПАРТНЕРОВ ==============

@dp.message(F.text.in_(["📊 Аналитика", "📊 Analitika"]))
async def show_analytics(message: types.Message):
    """Показать аналитику для партнера"""
    lang = db.get_user_language(message.from_user.id)
    user = db.get_user(message.from_user.id)
    
    if user[6] != 'seller':
        await message.answer(get_text(lang, 'not_seller'))
        return
    
    # Получаем магазины партнера
    stores = db.get_user_stores(message.from_user.id)
    
    if not stores:
        await message.answer(get_text(lang, 'no_stores'))
        return
    
    # Даем выбрать магазин для аналитики
    keyboard = InlineKeyboardBuilder()
    for store in stores:
        keyboard.button(text=f"📊 {store[2]}", callback_data=f"analytics_{store[0]}")
    keyboard.adjust(1)
    
    await message.answer(
        get_text(lang, 'select_store_for_analytics'),
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(F.data.startswith("analytics_"))
async def show_store_analytics(callback: types.CallbackQuery):
    """Показать подробную аналитику магазина"""
    store_id = int(callback.data.split("_")[1])
    lang = db.get_user_language(callback.from_user.id)
    
    # Получаем аналитику
    analytics = db.get_store_analytics(store_id)
    store = db.get_store(store_id)
    
    text = f"📊 <b>Аналитика магазина {store[2]}</b>\n\n"
    
    # Общая статистика
    text += "📈 <b>ОБЩАЯ СТАТИСТИКА</b>\n"
    text += f"📦 Всего бронирований: {analytics['total_bookings']}\n"
    text += f"✅ Выдано: {analytics['completed']}\n"
    text += f"❌ Отменено: {analytics['cancelled']}\n"
    text += f"💰 Конверсия: {analytics['conversion_rate']:.1f}%\n\n"
    
    # По дням недели
    if analytics['days_of_week']:
        text += f"📅 <b>ПО ДНЯМ НЕДЕЛИ</b>\n"
        # SQLite strftime('%w') -> 0=Вс, 1=Пн ... 6=Сб
        days_ru = ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб']
        for day, count in analytics['days_of_week'].items():
            text += f"{days_ru[day]}: {count} бронирований\n"
        text += "\n"
    
    # Популярные категории
    if analytics['popular_categories']:
        text += f"🏷 <b>ПОПУЛЯРНЫЕ КАТЕГОРИИ</b>\n"
        for cat, count in analytics['popular_categories'][:5]:
            text += f"{cat}: {count} бронирований\n"
        text += "\n"
    
    # Средний рейтинг
    if analytics.get('avg_rating'):
        text += f"⭐ <b>СРЕДНИЙ РЕЙТИНГ</b>\n"
        text += f"{analytics['avg_rating']:.1f}/5 ({analytics['rating_count']} отзывов)\n"
    
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

# ============== ПРОФИЛЬ ==============

@dp.message(F.text.contains("Профиль") | F.text.contains("Profil"))
async def profile(message: types.Message):
    lang = db.get_user_language(message.from_user.id)
    user = db.get_user(message.from_user.id)
    
    # ПРОВЕРКА: если пользователь удалил аккаунт
    if not user:
        await message.answer(
            get_text(lang, 'choose_language'),
            reply_markup=language_keyboard()
        )
        return
    
    # user: [0]user_id, [1]username, [2]first_name, [3]phone, [4]city, [5]language, [6]role, [7]is_admin, [8]notifications
    lang_text = 'Русский' if lang == 'ru' else 'Ozbekcha'
    
    text = f"{get_text(lang, 'your_profile')}\n\n"
    text += f"{get_text(lang, 'name')}: {user[2]}\n"
    text += f"{get_text(lang, 'phone')}: {user[3]}\n"
    text += f"{get_text(lang, 'city')}: {user[4]}\n"
    text += f"{get_text(lang, 'language')}: {lang_text}\n"
    
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=settings_keyboard(user[8], lang, role=user[6])
    )

@dp.callback_query(F.data == "profile_change_city")
async def profile_change_city(callback: types.CallbackQuery, state: FSMContext):
    """Запустить смену города из профиля"""
    lang = db.get_user_language(callback.from_user.id)
    try:
        await callback.message.edit_text(
            get_text(lang, 'choose_city'),
            parse_mode="HTML",
            reply_markup=city_keyboard(lang)
        )
    except Exception:
        await callback.message.answer(
            get_text(lang, 'choose_city'),
            parse_mode="HTML",
            reply_markup=city_keyboard(lang)
        )
    await state.set_state(ChangeCity.city)
    await callback.answer()

@dp.callback_query(F.data == "switch_to_customer")
async def switch_to_customer_cb(callback: types.CallbackQuery):
    """Переключиться в режим покупателя из профиля"""
    lang = db.get_user_language(callback.from_user.id)
    user_view_mode[callback.from_user.id] = 'customer'
    try:
        await callback.message.edit_text(get_text(lang, 'switched_to_customer'), reply_markup=main_menu_customer(lang))
    except Exception:
        await callback.message.answer(get_text(lang, 'switched_to_customer'), reply_markup=main_menu_customer(lang))
    await callback.answer()

@dp.callback_query(F.data == "become_partner_cb")
async def become_partner_cb(callback: types.CallbackQuery, state: FSMContext):
    """Стать партнером из профиля (inline)"""
    lang = db.get_user_language(callback.from_user.id)
    user = db.get_user(callback.from_user.id)
    if not user:
        await callback.message.answer(get_text(lang, 'choose_language'), reply_markup=language_keyboard())
        await callback.answer()
        return
    
    if user[6] == 'seller':
        stores = db.get_user_stores(callback.from_user.id)
        # Проверяем наличие ОДОБРЕННОГО магазина
        approved_stores = [s for s in stores if s[4] == "active"]
        
        if approved_stores:
            # Есть одобренный магазин - переключаем в режим партнёра
            user_view_mode[callback.from_user.id] = 'seller'
            try:
                await callback.message.edit_text(
                    get_text(lang, 'switched_to_seller'), 
                    reply_markup=get_appropriate_menu(callback.from_user.id, lang)
                )
            except Exception:
                await callback.message.answer(
                    get_text(lang, 'switched_to_seller'), 
                    reply_markup=get_appropriate_menu(callback.from_user.id, lang)
                )
            await callback.answer()
            return
        elif stores:
            # Есть магазин, но не одобрен
            pending_stores = [s for s in stores if s[4] == "pending"]
            if pending_stores:
                await callback.answer(
                    "⏳ Ваш магазин на модерации. Ожидайте одобрения администратором.",
                    show_alert=True
                )
                return
            else:
                # Магазин отклонён - можно зарегистрировать новый
                db.update_user_role(callback.from_user.id, 'customer')
        else:
            # Нет магазинов вообще
            db.update_user_role(callback.from_user.id, 'customer')
    
    # Начинаем регистрацию партнера
    try:
        await callback.message.edit_text(get_text(lang, 'become_partner_text'), parse_mode="HTML", reply_markup=city_keyboard(lang))
    except Exception:
        await callback.message.answer(get_text(lang, 'become_partner_text'), parse_mode="HTML", reply_markup=city_keyboard(lang))
    await state.set_state(RegisterStore.city)
    await callback.answer()

@dp.callback_query(F.data == "change_language")
async def change_language(callback: types.CallbackQuery):
    lang = db.get_user_language(callback.from_user.id)
    await callback.message.answer(
        get_text(lang, 'choose_language'),
        reply_markup=language_keyboard()
    )
    await callback.answer()


# ============== НАСТРОЙКИ: УВЕДОМЛЕНИЯ / УДАЛЕНИЕ АККАУНТА ==============
@dp.callback_query(F.data == "toggle_notifications")
async def toggle_notifications_callback(callback: types.CallbackQuery):
    """Переключить уведомления пользователя и обновить клавиатуру настроек"""
    lang = db.get_user_language(callback.from_user.id)
    try:
        new_enabled = db.toggle_notifications(callback.from_user.id)
    except Exception as e:
        await callback.answer(get_text(lang, 'access_denied'), show_alert=True)
        return

    # Покажем уведомление и обновим клавиатуру настроек
    text = get_text(lang, 'notifications_enabled') if new_enabled else get_text(lang, 'notifications_disabled')
    # Determine role for proper settings keyboard
    user = db.get_user(callback.from_user.id)
    role = user[6] if user and len(user) > 6 else 'customer'
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=settings_keyboard(new_enabled, lang, role=role))
    except Exception:
        # Если не получилось редактировать (возможно это не то сообщение), просто отправим новый
        await callback.message.answer(text, reply_markup=settings_keyboard(new_enabled, lang, role=role))

    await callback.answer()


@dp.callback_query(F.data == "delete_account")
async def delete_account_prompt(callback: types.CallbackQuery):
    """Попросить подтверждение перед удалением аккаунта"""
    lang = db.get_user_language(callback.from_user.id)

    # Подтверждение с двумя кнопками (aiogram 3.x синтаксис)
    builder = InlineKeyboardBuilder()
    builder.button(text=get_text(lang, 'yes_delete'), callback_data="confirm_delete_yes")
    builder.button(text=get_text(lang, 'no_cancel'), callback_data="confirm_delete_no")
    builder.adjust(2)

    # Редактируем сообщение (или отправляем новое) с предупреждением
    try:
        await callback.message.edit_text(get_text(lang, 'confirm_delete_account'), parse_mode="HTML", reply_markup=builder.as_markup())
    except Exception:
        await callback.message.answer(get_text(lang, 'confirm_delete_account'), parse_mode="HTML", reply_markup=builder.as_markup())

    await callback.answer()


@dp.callback_query(F.data == "confirm_delete_yes")
async def confirm_delete_yes(callback: types.CallbackQuery):
    lang = db.get_user_language(callback.from_user.id)

    # Удаляем данные пользователя полностью
    try:
        db.delete_user(callback.from_user.id)
    except Exception as e:
        await callback.answer(get_text(lang, 'access_denied'), show_alert=True)
        return

    # Сообщаем об удалении и предлагаем зарегистрироваться заново
    try:
        await callback.message.edit_text(
            get_text(lang, 'account_deleted') + "\n\n" + get_text(lang, 'choose_language'),
            parse_mode="HTML",
            reply_markup=language_keyboard()
        )
    except Exception:
        await callback.message.answer(
            get_text(lang, 'account_deleted') + "\n\n" + get_text(lang, 'choose_language'),
            parse_mode="HTML",
            reply_markup=language_keyboard()
        )

    await callback.answer()


@dp.callback_query(F.data == "confirm_delete_no")
async def confirm_delete_no(callback: types.CallbackQuery):
    """Отмена удаления — возвращаем настройки"""
    lang = db.get_user_language(callback.from_user.id)
    user = db.get_user(callback.from_user.id)

    if not user:
        # На всякий случай — если пользователя уже нет
        await callback.message.edit_text(get_text(lang, 'account_deleted'))
        await callback.answer()
        return

    try:
        await callback.message.edit_text(get_text(lang, 'operation_cancelled'), reply_markup=settings_keyboard(user[8], lang, role=user[6]))
    except Exception:
        await callback.message.answer(get_text(lang, 'operation_cancelled'), reply_markup=settings_keyboard(user[8], lang, role=user[6]))

    await callback.answer()

# ============== РЕЖИМ ПОКУПАТЕЛЯ ==============

@dp.message(F.text.contains("Режим покупателя") | F.text.contains("Xaridor rejimi"))
async def switch_to_customer(message: types.Message):
    lang = db.get_user_language(message.from_user.id)
    # Remember that the user prefers customer view until changed
    user_view_mode[message.from_user.id] = 'customer'
    await message.answer(
        get_text(lang, 'switched_to_customer'),
        reply_markup=main_menu_customer(lang)
    )

# ============== СТАТИСТИКА ПАРТНЁРА "СЕГОДНЯ" ==============

@dp.message(F.text.contains("Сегодня") | F.text.contains("Bugun"))
async def partner_today_stats(message: types.Message):
    """Компактная статистика партнёра за сегодня"""
    lang = db.get_user_language(message.from_user.id)
    user = db.get_user(message.from_user.id)
    
    # Проверяем, что это партнёр
    if not user or user[6] != 'seller':
        await message.answer(get_text(lang, 'access_denied'))
        return
    
    stores = db.get_user_stores(message.from_user.id)
    if not stores:
        await message.answer(get_text(lang, 'no_stores'))
        return
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Собираем ID всех магазинов партнёра
    store_ids = [store[0] for store in stores]
    placeholders = ','.join('?' * len(store_ids))
    
    # Статистика за сегодня
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Заказы за сегодня
    cursor.execute(f'''
        SELECT COUNT(*), SUM(b.quantity), SUM(o.discount_price * b.quantity)
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE o.store_id IN ({placeholders})
        AND DATE(b.created_at) = ?
        AND b.status != 'cancelled'
    ''', (*store_ids, today))
    
    orders_count, items_sold, revenue = cursor.fetchone()
    orders_count = orders_count or 0
    items_sold = int(items_sold or 0)
    revenue = int(revenue or 0)
    
    # Активные товары
    cursor.execute(f'''
        SELECT COUNT(*)
        FROM offers
        WHERE store_id IN ({placeholders})
        AND status = 'active'
    ''', store_ids)
    active_offers = cursor.fetchone()[0]
    
    # ТОП товар
    cursor.execute(f'''
        SELECT o.title, COUNT(*) as cnt
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE o.store_id IN ({placeholders})
        AND DATE(b.created_at) = ?
        AND b.status != 'cancelled'
        GROUP BY o.title
        ORDER BY cnt DESC
        LIMIT 1
    ''', (*store_ids, today))
    
    top_item = cursor.fetchone()
    top_item_text = f"\n🏆 ТОП товар: {top_item[0]} ({top_item[1]} заказов)" if top_item else ""
    
    conn.close()
    
    text = f"""📊 <b>СТАТИСТИКА СЕГОДНЯ</b>

💰 Выручка: {revenue:,} сум
📦 Товаров продано: {items_sold} шт
🛒 Заказов: {orders_count}
📋 Активных товаров: {active_offers}{top_item_text}

Обновлено: {datetime.now().strftime('%H:%M')}
"""
    
    await message.answer(text, parse_mode="HTML")

# ============== АДМИН ПАНЕЛЬ - INLINE CALLBACKS ==============

@dp.callback_query(F.data == "admin_refresh_dashboard")
async def refresh_dashboard(callback: types.CallbackQuery):
    """Обновить dashboard"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    # Используем тот же код что и в admin_dashboard
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # [Копируем весь код из admin_dashboard для получения статистики]
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "seller"')
    sellers = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "customer"')
    customers = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "active"')
    active_stores = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "pending"')
    pending_stores = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "active"')
    active_offers = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "inactive"')
    inactive_offers = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bookings')
    total_bookings = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "pending"')
    pending_bookings = cursor.fetchone()[0]
    
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE DATE(created_at) = ?', (today,))
    today_bookings = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT SUM(o.discount_price * b.quantity)
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE DATE(b.created_at) = ? AND b.status != 'cancelled'
    ''', (today,))
    today_revenue = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT COUNT(*) FROM users WHERE DATE(created_at) = ?', (today,))
    today_users = cursor.fetchone()[0]
    
    conn.close()
    
    text = "📊 <b>Dashboard - Общая статистика</b>\n\n"
    text += "👥 <b>Пользователи:</b>\n"
    text += f"├ Всего: {total_users} (+{today_users} сегодня)\n"
    text += f"├ 🏪 Партнёры: {sellers}\n"
    text += f"└ 🛍 Покупатели: {customers}\n\n"
    text += "🏪 <b>Магазины:</b>\n"
    text += f"├ ✅ Активные: {active_stores}\n"
    text += f"└ ⏳ На модерации: {pending_stores}\n\n"
    text += "📦 <b>Товары:</b>\n"
    text += f"├ ✅ Активные: {active_offers}\n"
    text += f"└ ❌ Неактивные: {inactive_offers}\n\n"
    text += "🎫 <b>Бронирования:</b>\n"
    text += f"├ Всего: {total_bookings}\n"
    text += f"├ ⏳ Активные: {pending_bookings}\n"
    text += f"└ 📅 Сегодня: {today_bookings}\n\n"
    text += f"💰 <b>Выручка сегодня:</b> {int(today_revenue):,} сум"
    
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    if pending_stores > 0:
        kb.button(text=f"⏳ Модерация ({pending_stores})", callback_data="admin_moderation")
    kb.button(text="📊 Детальная статистика", callback_data="admin_detailed_stats")
    kb.button(text="🔄 Обновить", callback_data="admin_refresh_dashboard")
    kb.adjust(1)
    
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    except Exception:
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())
    
    await callback.answer("✅ Обновлено")

@dp.callback_query(F.data == "admin_moderation")
async def admin_moderation_callback(callback: types.CallbackQuery):
    """Показать заявки на модерацию"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    
    lang = 'ru'
    pending = db.get_pending_stores()
    
    if not pending:
        await bot.send_message(callback.message.chat.id, get_text(lang, 'no_pending_stores'))
        return
    
    await bot.send_message(callback.message.chat.id, get_text(lang, 'pending_stores_count', count=len(pending)))
    
    for store in pending:
        text = f"🏪 <b>{store[2]}</b>\n\n"
        text += f"От: {store[8]} (@{store[9] or 'нет'})\n"
        text += f"ID: <code>{store[1]}</code>\n\n"
        text += f"📍 {store[3]}, {store[4]}\n"
        text += f"🏷 {store[6]}\n"
        text += f"📱 {store[7]}\n"
        text += f"📝 {store[5]}\n"
        text += f"📅 {store[10]}"
        
        await bot.send_message(
            callback.message.chat.id,
            text,
            parse_mode="HTML",
            reply_markup=moderation_keyboard(store[0])
        )
        await asyncio.sleep(0.3)

@dp.callback_query(F.data == "admin_detailed_stats")
async def admin_detailed_stats_callback(callback: types.CallbackQuery):
    """Детальная статистика из Dashboard"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    
    lang = 'ru'
    await bot.send_message(callback.message.chat.id, "⏳ Собираю статистику...")
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Статистика по пользователям
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "seller"')
    sellers = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "customer"')
    customers = cursor.fetchone()[0]
    
    # Статистика по магазинам
    cursor.execute('SELECT COUNT(*) FROM stores')
    total_stores = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "active"')
    approved_stores = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "pending"')
    pending_stores = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "rejected"')
    rejected_stores = cursor.fetchone()[0]
    
    # Статистика по городам
    cursor.execute('SELECT city, COUNT(*) FROM stores GROUP BY city ORDER BY COUNT(*) DESC LIMIT 5')
    top_cities = cursor.fetchall()
    
    # Статистика по категориям
    cursor.execute('SELECT category, COUNT(*) FROM stores GROUP BY category ORDER BY COUNT(*) DESC LIMIT 5')
    top_categories = cursor.fetchall()
    
    # Статистика по предложениям
    cursor.execute('SELECT COUNT(*) FROM offers')
    total_offers = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "active"')
    active_offers = cursor.fetchone()[0]
    cursor.execute('SELECT SUM(original_price) FROM offers WHERE status = "active"')
    total_original_price = cursor.fetchone()[0] or 0
    cursor.execute('SELECT SUM(discount_price) FROM offers WHERE status = "active"')
    total_discounted_price = cursor.fetchone()[0] or 0
    
    # Статистика по бронированиям
    cursor.execute('SELECT COUNT(*) FROM bookings')
    total_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "active"')
    active_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "completed"')
    completed_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "cancelled"')
    cancelled_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT SUM(quantity) FROM bookings WHERE status IN ("active", "completed")')
    total_quantity = cursor.fetchone()[0] or 0
    
    # Доход (экономия покупателей)
    cursor.execute('''
        SELECT SUM((o.original_price - o.discount_price) * b.quantity)
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE b.status IN ("active", "completed")
    ''')
    total_savings = cursor.fetchone()[0] or 0
    
    # Самые активные магазины
    cursor.execute('''
        SELECT s.name, COUNT(b.booking_id) as bookings_count
        FROM stores s
        LEFT JOIN offers o ON s.store_id = o.store_id
        LEFT JOIN bookings b ON o.offer_id = b.offer_id
        WHERE b.status IN ("active", "completed")
        GROUP BY s.store_id
        ORDER BY bookings_count DESC
        LIMIT 5
    ''')
    top_stores = cursor.fetchall()
    
    conn.close()
    
    # Формируем текст
    text = "📈 <b>ДЕТАЛЬНАЯ АНАЛИТИКА</b>\n\n"
    
    text += "👥 <b>ПОЛЬЗОВАТЕЛИ:</b>\n"
    text += f"├ Всего: {total_users}\n"
    text += f"├ Партнёры: {sellers}\n"
    text += f"└ Покупатели: {customers}\n\n"
    
    text += "🏪 <b>МАГАЗИНЫ:</b>\n"
    text += f"├ Всего: {total_stores}\n"
    text += f"├ ✅ Активные: {approved_stores}\n"
    text += f"├ ⏳ На модерации: {pending_stores}\n"
    text += f"└ ❌ Отклонённые: {rejected_stores}\n\n"
    
    if top_cities:
        text += "📍 <b>ТОП ГОРОДА:</b>\n"
        for city, count in top_cities:
            text += f"├ {city}: {count}\n"
        text += "\n"
    
    if top_categories:
        text += "🏷 <b>ТОП КАТЕГОРИИ:</b>\n"
        for cat, count in top_categories:
            text += f"├ {cat}: {count}\n"
        text += "\n"
    
    text += "📦 <b>ПРЕДЛОЖЕНИЯ:</b>\n"
    text += f"├ Всего: {total_offers}\n"
    text += f"├ Активные: {active_offers}\n"
    text += f"├ Общая стоимость: {int(total_original_price):,} сум\n"
    text += f"└ Со скидкой: {int(total_discounted_price):,} сум\n\n"
    
    text += "📋 <b>БРОНИРОВАНИЯ:</b>\n"
    text += f"├ Всего: {total_bookings}\n"
    text += f"├ ⏳ Активные: {active_bookings}\n"
    text += f"├ ✅ Завершённые: {completed_bookings}\n"
    text += f"├ ❌ Отменённые: {cancelled_bookings}\n"
    text += f"└ Забронировано товаров: {total_quantity} шт\n\n"
    
    text += f"💰 <b>ЭКОНОМИЯ ПОКУПАТЕЛЕЙ:</b> {int(total_savings):,} сум\n\n"
    
    if top_stores:
        text += "🏆 <b>ТОП МАГАЗИНЫ:</b>\n"
        for store_name, count in top_stores:
            text += f"├ {store_name}: {count} бронирований\n"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML")

@dp.callback_query(F.data == "admin_list_sellers")
async def admin_list_sellers_callback(callback: types.CallbackQuery):
    """Полный список продавцов с детальной информацией"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Получаем всех продавцов с дополнительной информацией
    cursor.execute('''
        SELECT u.user_id, u.username, u.first_name, u.city, u.created_at,
               COUNT(DISTINCT s.store_id) as stores_count,
               COUNT(DISTINCT CASE WHEN s.status = 'active' THEN s.store_id END) as active_stores,
               COUNT(DISTINCT o.offer_id) as offers_count
        FROM users u
        LEFT JOIN stores s ON u.user_id = s.owner_id
        LEFT JOIN offers o ON s.store_id = o.store_id AND o.status = 'active'
        WHERE u.role = 'seller'
        GROUP BY u.user_id
        ORDER BY active_stores DESC, offers_count DESC
    ''')
    sellers = cursor.fetchall()
    conn.close()
    
    if not sellers:
        await bot.send_message(callback.message.chat.id, "👥 Продавцов нет")
        return
    
    text = f"👥 <b>Список партнёров ({len(sellers)}):</b>\n\n"
    
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    
    for user_id, username, first_name, city, created_at, stores_count, active_stores, offers_count in sellers[:20]:
        text += f"👤 <b>{first_name or 'Без имени'}</b>"
        if username:
            text += f" (@{username})"
        text += f"\n"
        text += f"├ 📍 {city or 'Не указан'}\n"
        text += f"├ 🏪 Магазинов: {active_stores}/{stores_count}\n"
        text += f"├ 📦 Активных товаров: {offers_count}\n"
        text += f"└ ID: <code>{user_id}</code>\n"
        
        # Кнопка удаления магазинов партнёра
        if stores_count > 0:
            kb.button(text=f"🗑 Удалить магазины {first_name or user_id}", callback_data=f"admin_delete_user_stores_{user_id}")
        text += "\n"
    
    kb.adjust(1)
    
    if len(sellers) > 20:
        text += f"\n<i>Показано 20 из {len(sellers)}. Используйте поиск для остальных.</i>"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML", reply_markup=kb.as_markup() if kb.export() else None)

@dp.callback_query(F.data.startswith("admin_delete_user_stores_"))
async def admin_delete_user_stores_callback(callback: types.CallbackQuery):
    """Удаление всех магазинов пользователя"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Получаем информацию о пользователе и его магазинах
    cursor.execute('SELECT first_name, username FROM users WHERE user_id = ?', (user_id,))
    user_info = cursor.fetchone()
    
    if not user_info:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        conn.close()
        return
    
    first_name, username = user_info
    
    # Получаем список магазинов
    cursor.execute('SELECT store_id, name, status FROM stores WHERE owner_id = ?', (user_id,))
    stores = cursor.fetchall()
    
    if not stores:
        await callback.answer("❌ У пользователя нет магазинов", show_alert=True)
        conn.close()
        return
    
    # Подтверждение удаления
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, удалить все", callback_data=f"admin_confirm_delete_stores_{user_id}")
    kb.button(text="❌ Отмена", callback_data="admin_cancel_action")
    kb.adjust(1)
    
    text = f"⚠️ <b>Подтверждение удаления</b>\n\n"
    text += f"Пользователь: {first_name or 'Без имени'}"
    if username:
        text += f" (@{username})"
    text += f"\n\nМагазины ({len(stores)}):\n"
    
    for store_id, name, status in stores:
        status_emoji = "✅" if status == "active" else "⏳" if status == "pending" else "❌"
        text += f"{status_emoji} {name}\n"
    
    text += f"\n<b>Вы уверены, что хотите удалить все магазины этого пользователя?</b>"
    
    conn.close()
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_confirm_delete_stores_"))
async def admin_confirm_delete_stores_callback(callback: types.CallbackQuery):
    """Подтверждение удаления магазинов"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    user_id = int(callback.data.split("_")[-1])
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Получаем магазины
    cursor.execute('SELECT store_id FROM stores WHERE owner_id = ?', (user_id,))
    stores = cursor.fetchall()
    
    if not stores:
        await callback.answer("❌ Магазины не найдены", show_alert=True)
        conn.close()
        return
    
    # Удаляем все товары магазинов
    for (store_id,) in stores:
        cursor.execute('UPDATE offers SET status = "deleted" WHERE store_id = ?', (store_id,))
    
    # Удаляем магазины (меняем статус на rejected)
    cursor.execute('UPDATE stores SET status = "rejected" WHERE owner_id = ?', (user_id,))
    
    # Меняем роль пользователя на customer
    cursor.execute('UPDATE users SET role = "customer" WHERE user_id = ?', (user_id,))
    
    conn.commit()
    conn.close()
    
    await callback.message.edit_text(
        f"✅ <b>Успешно удалено</b>\n\n"
        f"Удалено магазинов: {len(stores)}\n"
        f"Все товары этих магазинов деактивированы\n"
        f"Пользователь переведён в роль покупателя",
        parse_mode="HTML"
    )
    await callback.answer("✅ Магазины удалены")

@dp.callback_query(F.data == "admin_cancel_action")
async def admin_cancel_action_callback(callback: types.CallbackQuery):
    """Отмена действия"""
    await callback.message.delete()
    await callback.answer("Действие отменено")

@dp.callback_query(F.data == "admin_search_user")
async def admin_search_user_callback(callback: types.CallbackQuery):
     """Поиск пользователя"""
     await callback.answer("🔍 Отправьте ID или username пользователя для поиска", show_alert=True)

@dp.callback_query(F.data == "admin_approved_stores")
async def admin_approved_stores_callback(callback: types.CallbackQuery):
    """Полный список одобренных магазинов"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT s.store_id, s.name, s.city, s.category, u.first_name, u.username,
               s.created_at, COUNT(o.offer_id) as offers_count
        FROM stores s
        JOIN users u ON s.owner_id = u.user_id
        LEFT JOIN offers o ON s.store_id = o.store_id AND o.status = 'active'
        WHERE s.status = 'active'
        GROUP BY s.store_id
        ORDER BY s.created_at DESC
    ''')
    stores = cursor.fetchall()
    conn.close()
    
    if not stores:
        await bot.send_message(callback.message.chat.id, "🏪 Одобренных магазинов нет")
        return
    
    text = f"🏪 <b>Одобренные магазины ({len(stores)}):</b>\n\n"
    
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    
    for store_id, name, city, category, owner_name, username, created_at, offers_count in stores[:15]:
        text += f"🏪 <b>{name}</b>\n"
        text += f"├ 📍 {city} | 🏷 {category}\n"
        text += f"├ 👤 {owner_name}"
        if username:
            text += f" (@{username})"
        text += f"\n├ 📦 Товаров: {offers_count}\n"
        text += f"└ ID: <code>{store_id}</code>\n"
        
        # Добавляем кнопку блокировки магазина
        kb.button(text=f"🚫 Заблокировать {name[:15]}", callback_data=f"admin_block_store_{store_id}")
        text += "\n"
    
    kb.adjust(1)
    
    if len(stores) > 15:
        text += f"\n<i>Показано 15 из {len(stores)}</i>"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML", reply_markup=kb.as_markup() if kb.export() else None)

@dp.callback_query(F.data.startswith("admin_block_store_"))
async def admin_block_store_callback(callback: types.CallbackQuery):
    """Блокировка магазина"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    store_id = int(callback.data.split("_")[-1])
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT name FROM stores WHERE store_id = ?', (store_id,))
    store = cursor.fetchone()
    
    if not store:
        await callback.answer("❌ Магазин не найден", show_alert=True)
        conn.close()
        return
    
    # Блокируем магазин
    cursor.execute('UPDATE stores SET status = "rejected" WHERE store_id = ?', (store_id,))
    
    # Деактивируем все товары
    cursor.execute('UPDATE offers SET status = "inactive" WHERE store_id = ?', (store_id,))
    
    conn.commit()
    conn.close()
    
    await callback.message.edit_text(
        f"🚫 <b>Магазин заблокирован</b>\n\n"
        f"Название: {store[0]}\n"
        f"ID: {store_id}\n\n"
        f"Все товары этого магазина деактивированы.",
        parse_mode="HTML"
    )
    await callback.answer("✅ Магазин заблокирован")

@dp.callback_query(F.data == "admin_rejected_stores")
async def admin_rejected_stores_callback(callback: types.CallbackQuery):
    """Отклонённые магазины"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT s.store_id, s.name, s.city, u.first_name, u.username, s.created_at
        FROM stores s
        JOIN users u ON s.owner_id = u.user_id
        WHERE s.status = 'rejected'
        ORDER BY s.created_at DESC
        LIMIT 10
    ''')
    stores = cursor.fetchall()
    conn.close()
    
    if not stores:
        await bot.send_message(callback.message.chat.id, "🏪 Отклонённых магазинов нет")
        return
    
    text = f"❌ <b>Отклонённые магазины ({len(stores)}):</b>\n\n"
    
    for store_id, name, city, owner_name, username, created_at in stores:
        text += f"🏪 {name}\n"
        text += f"├ 📍 {city}\n"
        text += f"├ 👤 {owner_name}"
        if username:
            text += f" (@{username})"
        text += f"\n└ ID: <code>{store_id}</code>\n\n"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML")

@dp.callback_query(F.data == "admin_search_store")
async def admin_search_store_callback(callback: types.CallbackQuery):
    """Поиск магазина"""
    await callback.answer("🔍 Функция поиска будет добавлена позже", show_alert=True)

@dp.callback_query(F.data == "admin_all_offers")
async def admin_all_offers_callback(callback: types.CallbackQuery):
    """Детальный список всех товаров"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT o.offer_id, o.title, o.original_price, o.discount_price, o.quantity,
               s.name as store_name, o.status, o.created_at
        FROM offers o
        JOIN stores s ON o.store_id = s.store_id
        ORDER BY o.created_at DESC
        LIMIT 20
    ''')
    offers = cursor.fetchall()
    
    cursor.execute('SELECT COUNT(*) FROM offers')
    total = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "active"')
    active = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "deleted"')
    deleted = cursor.fetchone()[0]
    
    conn.close()
    
    text = f"📦 <b>Все товары</b>\n\n"
    text += f"📊 Статистика:\n"
    text += f"├ Всего: {total}\n"
    text += f"├ ✅ Активных: {active}\n"
    text += f"└ 🗑 Удалённых: {deleted}\n\n"
    
    if offers:
        text += "<b>Последние товары:</b>\n\n"
        for offer_id, title, orig, disc, qty, store, status, created in offers[:10]:
            status_emoji = "✅" if status == "active" else "❌"
            text += f"{status_emoji} <b>{title}</b>\n"
            text += f"├ 🏪 {store}\n"
            text += f"├ 💰 {int(orig):,} → {int(disc):,} сум\n"
            text += f"├ 📦 Остаток: {qty}\n"
            text += f"└ ID: <code>{offer_id}</code>\n\n"
        
        if len(offers) > 10:
            text += f"<i>Показано 10 из {len(offers)}</i>"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML")

@dp.callback_query(F.data == "admin_cleanup_offers")
async def admin_cleanup_offers_callback(callback: types.CallbackQuery):
    """Очистка истекших и удалённых товаров"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Подсчёт истекших товаров
    today = get_uzb_time().strftime('%Y-%m-%d')
    cursor.execute('SELECT COUNT(*) FROM offers WHERE expiry_date < ? AND status = "active"', (today,))
    expired = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "deleted"')
    deleted = cursor.fetchone()[0]
    
    conn.close()
    
    text = f"🗑 <b>Очистка товаров</b>\n\n"
    text += f"📊 Найдено:\n"
    text += f"├ ⏰ Истекших: {expired}\n"
    text += f"└ 🗑 Удалённых: {deleted}\n\n"
    
    if expired + deleted > 0:
        text += "<i>Функция автоочистки будет добавлена в следующем обновлении</i>"
    else:
        text += "✅ Все товары актуальны!"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML")

@dp.callback_query(F.data == "admin_pending_bookings")
async def admin_pending_bookings_callback(callback: types.CallbackQuery):
    """Детальный список активных бронирований"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT b.booking_id, o.title, b.quantity, u.first_name, s.name,
               b.created_at, (o.original_price - o.discount_price) * b.quantity as savings
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        JOIN users u ON b.user_id = u.user_id
        JOIN stores s ON o.store_id = s.store_id
        WHERE b.status = 'active'
        ORDER BY b.created_at DESC
        LIMIT 15
    ''')
    bookings = cursor.fetchall()
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "active"')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT SUM(quantity) FROM bookings WHERE status = "active"')
    total_qty = cursor.fetchone()[0] or 0
    
    conn.close()
    
    text = f"📋 <b>Активные бронирования</b>\n\n"
    text += f"📊 Всего: {total} ({total_qty} шт.)\n\n"
    
    if bookings:
        for booking_id, title, qty, customer, store, created, savings in bookings[:10]:
            text += f"🎫 <b>{title}</b> ({qty} шт.)\n"
            text += f"├ 👤 {customer}\n"
            text += f"├ 🏪 {store}\n"
            text += f"├ 💰 Экономия: {int(savings):,} сум\n"
            text += f"└ ID: <code>{booking_id}</code>\n\n"
        
        if len(bookings) > 10:
            text += f"<i>Показано 10 из {len(bookings)}</i>"
    else:
        text += "📭 Активных бронирований нет"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML")

@dp.callback_query(F.data == "admin_completed_bookings")
async def admin_completed_bookings_callback(callback: types.CallbackQuery):
    """Завершённые бронирования"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT b.booking_id, o.title, b.quantity, u.first_name, s.name,
               b.created_at, (o.original_price - o.discount_price) * b.quantity as savings
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        JOIN users u ON b.user_id = u.user_id
        JOIN stores s ON o.store_id = s.store_id
        WHERE b.status = 'completed'
        ORDER BY b.created_at DESC
        LIMIT 10
    ''')
    bookings = cursor.fetchall()
    
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "completed"')
    total = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT SUM((o.original_price - o.discount_price) * b.quantity)
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE b.status = 'completed'
    ''')
    total_savings = cursor.fetchone()[0] or 0
    
    conn.close()
    
    text = f"✅ <b>Завершённые бронирования</b>\n\n"
    text += f"📊 Всего: {total}\n"
    text += f"💰 Общая экономия: {int(total_savings):,} сум\n\n"
    
    if bookings:
        for booking_id, title, qty, customer, store, created, savings in bookings[:8]:
            text += f"✅ {title} ({qty} шт.)\n"
            text += f"├ {customer} | {store}\n"
            text += f"└ 💰 {int(savings):,} сум\n\n"
        
        if len(bookings) > 8:
            text += f"<i>Показано 8 из {len(bookings)}</i>"
    else:
        text += "📭 Завершённых бронирований нет"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML")

@dp.callback_query(F.data == "admin_bookings_stats")
async def admin_bookings_stats_callback(callback: types.CallbackQuery):
    """Детальная статистика бронирований"""
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Доступ запрещён", show_alert=True)
        return
    
    await callback.answer()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Общая статистика
    cursor.execute('SELECT COUNT(*) FROM bookings')
    total = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "active"')
    active = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "completed"')
    completed = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "cancelled"')
    cancelled = cursor.fetchone()[0]
    
    # Экономия
    cursor.execute('''
        SELECT SUM((o.original_price - o.discount_price) * b.quantity)
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE b.status IN ('active', 'completed')
    ''')
    total_savings = cursor.fetchone()[0] or 0
    
    # Топ магазинов по бронированиям
    cursor.execute('''
        SELECT s.name, COUNT(b.booking_id) as cnt
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        JOIN stores s ON o.store_id = s.store_id
        WHERE b.status IN ('active', 'completed')
        GROUP BY s.store_id
        ORDER BY cnt DESC
        LIMIT 5
    ''')
    top_stores = cursor.fetchall()
    
    # Топ покупателей
    cursor.execute('''
        SELECT u.first_name, COUNT(b.booking_id) as cnt
        FROM bookings b
        JOIN users u ON b.user_id = u.user_id
        GROUP BY u.user_id
        ORDER BY cnt DESC
        LIMIT 5
    ''')
    top_customers = cursor.fetchall()
    
    conn.close()
    
    text = f"📋 <b>Статистика бронирований</b>\n\n"
    text += f"📊 <b>Общее:</b>\n"
    text += f"├ Всего: {total}\n"
    text += f"├ ⏳ Активных: {active}\n"
    text += f"├ ✅ Завершённых: {completed}\n"
    text += f"└ ❌ Отменённых: {cancelled}\n\n"
    
    text += f"💰 <b>Экономия покупателей:</b> {int(total_savings):,} сум\n\n"
    
    if top_stores:
        text += "🏆 <b>Топ магазины:</b>\n"
        for name, cnt in top_stores:
            text += f"├ {name}: {cnt}\n"
        text += "\n"
    
    if top_customers:
        text += "👥 <b>Топ покупатели:</b>\n"
        for name, cnt in top_customers:
            text += f"├ {name or 'Без имени'}: {cnt}\n"
    
    await bot.send_message(callback.message.chat.id, text, parse_mode="HTML")

# ============== АДМИН ПАНЕЛЬ - ОБРАБОТЧИКИ (продолжение) ==============

@dp.message(F.text == "📈 Аналитика")
async def admin_analytics(message: types.Message):
    """Детальная аналитика системы"""
    lang = 'ru'
    if not db.is_admin(message.from_user.id):
        await message.answer(get_text(lang, 'access_denied'))
        return
    
    await message.answer("⏳ Собираю статистику...")
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Статистика по пользователям
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "seller"')
    sellers = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM users WHERE role = "customer"')
    customers = cursor.fetchone()[0]
    
    # Статистика по магазинам
    cursor.execute('SELECT COUNT(*) FROM stores')
    total_stores = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "active"')
    approved_stores = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "pending"')
    pending_stores = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "rejected"')
    rejected_stores = cursor.fetchone()[0]
    
    # Статистика по городам
    cursor.execute('SELECT city, COUNT(*) FROM stores GROUP BY city ORDER BY COUNT(*) DESC LIMIT 5')
    top_cities = cursor.fetchall()
    
    # Статистика по категориям
    cursor.execute('SELECT category, COUNT(*) FROM stores GROUP BY category ORDER BY COUNT(*) DESC LIMIT 5')
    top_categories = cursor.fetchall()
    
    # Статистика по предложениям
    cursor.execute('SELECT COUNT(*) FROM offers')
    total_offers = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "active"')
    active_offers = cursor.fetchone()[0]
    cursor.execute('SELECT SUM(original_price) FROM offers WHERE status = "active"')
    total_original_price = cursor.fetchone()[0] or 0
    cursor.execute('SELECT SUM(discount_price) FROM offers WHERE status = "active"')
    total_discounted_price = cursor.fetchone()[0] or 0
    
    # Статистика по бронированиям
    cursor.execute('SELECT COUNT(*) FROM bookings')
    total_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "active"')
    active_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "completed"')
    completed_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "cancelled"')
    cancelled_bookings = cursor.fetchone()[0]
    cursor.execute('SELECT SUM(quantity) FROM bookings WHERE status IN ("active", "completed")')
    total_quantity = cursor.fetchone()[0] or 0
    
    # Доход (экономия покупателей)
    cursor.execute('''
        SELECT SUM((o.original_price - o.discount_price) * b.quantity)
        FROM bookings b
        JOIN offers o ON b.offer_id = o.offer_id
        WHERE b.status IN ("active", "completed")
    ''')
    total_savings = cursor.fetchone()[0] or 0
    
    # Самые активные магазины
    cursor.execute('''
        SELECT s.name, COUNT(b.booking_id) as bookings_count
        FROM stores s
        LEFT JOIN offers o ON s.store_id = o.store_id
        LEFT JOIN bookings b ON o.offer_id = b.offer_id
        WHERE b.status IN ("active", "completed")
        GROUP BY s.store_id
        ORDER BY bookings_count DESC
        LIMIT 5
    ''')
    top_stores = cursor.fetchall()
    
    conn.close()
    
    # Формируем текстовый отчёт
    text = "📈 <b>ПОЛНАЯ СТАТИСТИКА СИСТЕМЫ</b>\n\n"
    
    text += "👥 <b>ПОЛЬЗОВАТЕЛИ</b>\n"
    text += f"Всего: {total_users}\n"
    text += f"🏪 Партнеров: {sellers}\n"
    text += f"🛍 Покупателей: {customers}\n\n"
    
    text += "🏪 <b>МАГАЗИНЫ</b>\n"
    text += f"Всего: {total_stores}\n"
    text += f"✅ Одобрено: {approved_stores}\n"
    text += f"⏳ На модерации: {pending_stores}\n"
    text += f"❌ Отклонено: {rejected_stores}\n\n"
    
    if top_cities:
        text += "📍 <b>ТОП-5 ГОРОДОВ</b>\n"
        for city, count in top_cities:
            text += f"• {city}: {count}\n"
        text += "\n"
    
    if top_categories:
        text += "🏷 <b>ТОП-5 КАТЕГОРИЙ</b>\n"
        for category, count in top_categories:
            text += f"• {category}: {count}\n"
        text += "\n"
    
    text += "🍽 <b>ПРЕДЛОЖЕНИЯ</b>\n"
    text += f"Всего: {total_offers}\n"
    text += f"✅ Активных: {active_offers}\n"
    text += f"💰 Общая стоимость: {int(total_original_price):,} сум\n"
    text += f"💸 Со скидкой: {int(total_discounted_price):,} сум\n\n"
    
    text += "📋 <b>БРОНИРОВАНИЯ</b>\n"
    text += f"Всего: {total_bookings}\n"
    text += f"✅ Активных: {active_bookings}\n"
    text += f"✔️ Завершен: {completed_bookings}\n"
    text += f"❌ Отменен: {cancelled_bookings}\n"
    text += f"📦 Товаров забронировано: {total_quantity} шт\n"
    text += f"💰 Экономия покупателей: {int(total_savings):,} сум\n\n"
    
    if top_stores:
        text += "🏆 <b>ТОП-5 МАГАЗИНОВ</b>\n"
        for store_name, bookings_count in top_stores:
            text += f"• {store_name}: {bookings_count} заказов\n"
    
    await message.answer(text, parse_mode="HTML")
    
    # Создаём CSV файл
    import csv
    from datetime import datetime
    from aiogram.types import FSInputFile
    
    filename = f"statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        
        # Заголовок
        writer.writerow(['ПОЛНАЯ СТАТИСТИКА FUDLY'])
        writer.writerow(['Дата создания', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow([])
        
        # Пользователи
        writer.writerow(['ПОЛЬЗОВАТЕЛИ'])
        writer.writerow(['Всего', total_users])
        writer.writerow(['Партнёров', sellers])
        writer.writerow(['Покупателей', customers])
        writer.writerow([])
        
        # Магазины
        writer.writerow(['МАГАЗИНЫ'])
        writer.writerow(['Всего', total_stores])
        writer.writerow(['Одобрено', approved_stores])
        writer.writerow(['На модерации', pending_stores])
        writer.writerow(['Отклонено', rejected_stores])
        writer.writerow([])
        
        # Города
        if top_cities:
            writer.writerow(['ТОП ГОРОДА'])
            writer.writerow(['Город', 'Количество'])
            for city, count in top_cities:
                writer.writerow([city, count])
            writer.writerow([])
        
        # Категории
        if top_categories:
            writer.writerow(['ТОП КАТЕГОРИИ'])
            writer.writerow(['Категория', 'Количество'])
            for category, count in top_categories:
                writer.writerow([category, count])
            writer.writerow([])
        
        # Предложения
        writer.writerow(['ПРЕДЛОЖЕНИЯ'])
        writer.writerow(['Всего', total_offers])
        writer.writerow(['Активных', active_offers])
        writer.writerow(['Общая стоимость (сум)', int(total_original_price)])
        writer.writerow(['Со скидкой (сум)', int(total_discounted_price)])
        writer.writerow([])
        
        # Бронирования
        writer.writerow(['БРОНИРОВАНИЯ'])
        writer.writerow(['Всего', total_bookings])
        writer.writerow(['Активных', active_bookings])
        writer.writerow(['Завершено', completed_bookings])
        writer.writerow(['Отменено', cancelled_bookings])
        writer.writerow(['Товаров забронировано', total_quantity])
        writer.writerow(['Экономия покупателей (сум)', int(total_savings)])
        writer.writerow([])
        
        # Топ магазины
        if top_stores:
            writer.writerow(['ТОП МАГАЗИНЫ'])
            writer.writerow(['Название', 'Заказов'])
            for store_name, bookings_count in top_stores:
                writer.writerow([store_name, bookings_count])
    
    # Отправляем файл
    document = FSInputFile(filename)
    await message.answer_document(
        document=document,
        caption="📊 Полная статистика в формате CSV"
    )
    
    # Удаляем файл после отправки
    import os
    os.remove(filename)

@dp.message(F.text == "🏪 Заявки на партнерство")
async def admin_pending_stores(message: types.Message):
    lang = 'ru'  # Админ-панель только на русском
    if not db.is_admin(message.from_user.id):
        await message.answer(get_text(lang, 'access_denied'))
        return
    
    pending = db.get_pending_stores()
    
    if not pending:
        await message.answer(get_text(lang, 'no_pending_stores'))
        return
    
    await message.answer(get_text(lang, 'pending_stores_count', count=len(pending)))
    
    for store in pending:
        text = f"🏪 <b>{store[2]}</b>\n\n"
        text += f"От: {store[8]} (@{store[9] or 'нет'})\n"
        text += f"ID: <code>{store[1]}</code>\n\n"
        text += f"📍 {store[3]}, {store[4]}\n"
        text += f"🏷 {store[6]}\n"
        text += f"📱 {store[7]}\n"
        text += f"📝 {store[5]}\n"
        text += f"📅 {store[10]}"
        
        await message.answer(
            text,
            parse_mode="HTML",
            reply_markup=moderation_keyboard(store[0])
        )
        await asyncio.sleep(0.3)

@dp.callback_query(F.data.startswith("approve_"))
async def approve_store(callback: types.CallbackQuery):
    lang = 'ru'  # Админ-панель на русском
    if not db.is_admin(callback.from_user.id):
        await callback.answer(get_text(lang, 'access_denied'), show_alert=True)
        return
    
    store_id = int(callback.data.split("_")[2])
    
    # Получаем данные магазина до одобрения
    store = db.get_store(store_id)
    if not store:
        await callback.answer("❌ Магазин не найден", show_alert=True)
        return
    
    owner_id = store[1]
    store_name = store[2]
    
    # Одобряем магазин (включает обновление роли владельца)
    success = db.approve_store(store_id)
    
    if success:
        # Уведомляем владельца
        try:
            owner_lang = db.get_user_language(owner_id)
            await bot.send_message(
                owner_id,
                get_text(owner_lang, 'store_approved'),
                parse_mode="HTML",
                reply_markup=main_menu_seller(owner_lang)
            )
        except Exception as e:
            logger.error(f"Failed to notify store owner {owner_id}: {e}")
        
        # Обновляем сообщение
        await callback.message.edit_text(
            callback.message.text + "\n\n✅ <b>ОДОБРЕНО</b>",
            parse_mode="HTML"
        )
        await callback.answer(f"✅ Магазин '{store_name}' одобрен!")
    else:
        await callback.answer("❌ Ошибка при одобрении магазина", show_alert=True)

@dp.callback_query(F.data.startswith("reject_"))
async def reject_store(callback: types.CallbackQuery):
    lang = 'ru'  # Админ-панель на русском
    if not db.is_admin(callback.from_user.id):
        await callback.answer(get_text(lang, 'access_denied'), show_alert=True)
        return
    
    store_id = int(callback.data.split("_")[2])
    db.reject_store(store_id, "Не соответствует требованиям")
    
    # Уведомляем владельца
    store = db.get_store(store_id)
    if store:
        owner_id = store[1]
        try:
            owner_lang = db.get_user_language(owner_id)
            await bot.send_message(
                owner_id,
                get_text(owner_lang, 'store_rejected'),
                parse_mode="HTML"
            )
        except Exception:
            pass
    
    await callback.message.edit_text(
        callback.message.text + "\n\n❌ <b>ОТКЛОНЕНО</b>",
        parse_mode="HTML"
    )
    await callback.answer(get_text(lang, 'store_rejected_admin'))

@dp.message(F.text == "📋 Все предложения")
async def admin_all_offers(message: types.Message):
    lang = 'ru'
    if not db.is_admin(message.from_user.id):
        await message.answer(get_text(lang, 'access_denied'))
        return
    
    offers = db.get_active_offers()
    if not offers:
        await message.answer(f"📋 <b>{get_text(lang, 'all_offers')}</b>\n\n{get_text(lang, 'no_active_offers')}", parse_mode="HTML")
        return
    
    text = f"📋 <b>{get_text(lang, 'all_offers')} ({len(offers)})</b>\n\n"
    
    for i, offer in enumerate(offers[:10]):  # Показываем первые 10
        discount_percent = int((1 - offer[5] / offer[4]) * 100) if offer[4] > 0 else 0
        
        # Получаем единицы измерения
        # unit находится в [13] если есть, иначе используем по умолчанию
        # После JOIN структура: [0-12] offers базовые, [13]unit (если есть), [14]category (если есть)
        # [15] или [13]store_name, [16] или [14]address, [17] или [15]city
        if len(offer) >= 19:
            # Новая структура с unit/category
            unit = offer[13] if offer[13] else 'шт'
        else:
            # Старая структура без unit/category
            unit = 'шт'
        
        text += f"{i+1}. <b>{offer[2]}</b>\n"
        text += f"   💰 {int(offer[4]):,} ➜ {int(offer[5]):,} сум (-{discount_percent}%)\n"
        text += f"   📦 {offer[6]} {unit}\n"
        
        # Показываем срок годности
        if len(offer) > 12 and offer[12]:
            text += f"   📅 До: {offer[12]}\n"
        
        # Показываем магазин (индексы для JOIN: 15-store_name, 17-city)
        if len(offer) > 17:
            # Индексы зависят от наличия unit/category
            if len(offer) >= 19:
                # Новая структура: [15]store_name, [17]city
                store_name = offer[15] if len(offer) > 15 else ""
                city = offer[17] if len(offer) > 17 else ""
            else:
                # Старая структура: [13]store_name, [15]city
                store_name = offer[13] if len(offer) > 13 else ""
                city = offer[15] if len(offer) > 15 else ""
            if store_name or city:
                text += f"🏪 {store_name} ({city})\n"
        text += "\n"
    
    if len(offers) > 10:
        text += f"... и еще {len(offers) - 10} предложений"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "🏪 Все магазины")
async def admin_all_stores(message: types.Message):
    lang = 'ru'
    if not db.is_admin(message.from_user.id):
        await message.answer(get_text(lang, 'access_denied'))
        return
    
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM stores ORDER BY created_at DESC')
    stores = cursor.fetchall()
    conn.close()
    
    if not stores:
        await message.answer("Магазинов нет")
        return
    
    await message.answer(f"🏪 <b>Все магазины ({len(stores)})</b>", parse_mode="HTML")
    
    for store in stores[:20]:
        status_emoji = {
            'active': '✅',
            'approved': '✅',
            'pending': '⏳',
            'rejected': '❌'
        }.get(store[8], '❓')
        
        text = f"{status_emoji} <b>{store[2]}</b>\n"
        text += f"ID: {store[0]}\n"
        text += f"📍 {store[3]}, {store[4]}\n"
        text += f"🏷 {store[6]}\n"
        text += f"Статус: {store[8]}"
        
        # Создаем inline кнопку для удаления
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        builder = InlineKeyboardBuilder()
        builder.button(text="🗑 Удалить магазин", callback_data=f"delete_store_{store[0]}")
        
        await message.answer(text, parse_mode="HTML", reply_markup=builder.as_markup())
        await asyncio.sleep(0.2)

@dp.callback_query(F.data.startswith("delete_store_"))
async def delete_store_callback(request: types.CallbackQuery):
    lang = 'ru'
    if not db.is_admin(request.from_user.id):
        await request.answer(get_text(lang, 'access_denied'), show_alert=True)
        return
    
    store_id = int(request.data.split("_")[2])
    
    try:
        db.delete_store(store_id)
        await request.message.edit_text(
            request.message.text + "\n\n🗑 <b>УДАЛЕНО</b>",
            parse_mode="HTML"
        )
        await request.answer("✅ Магазин удалён!")
    except Exception as e:
        await request.answer(f"❌ Ошибка: {str(e)}", show_alert=True)

@dp.message(F.text == "📢 Рассылка")
async def admin_broadcast(message: types.Message):
    lang = 'ru'
    if not db.is_admin(message.from_user.id):
        await message.answer(get_text(lang, 'access_denied'))
        return
    
    await message.answer("📢 Функция рассылки в разработке")

@dp.message(F.text == "⚙️ Настройки")
async def admin_settings(message: types.Message):
    lang = 'ru'
    if not db.is_admin(message.from_user.id):
        await message.answer(get_text(lang, 'access_denied'))
        return
    
    await message.answer("⚙️ Настройки админа в разработке")

# ============== ОТЛАДКА - НЕИЗВЕСТНЫЕ СООБЩЕНИЯ ==============

@dp.message(F.text)
async def unknown_message_debug(message: types.Message):
    """Отладочный обработчик для неизвестных текстовых сообщений"""
    print(f"⚠️ НЕИЗВЕСТНОЕ СООБЩЕНИЕ от {message.from_user.id}: '{message.text}'")
    print(f"   Длина текста: {len(message.text)}")
    print(f"   Байты: {message.text.encode('utf-8')}")

# ============== ЗАПУСК БОТА ==============

# ============================================
# ФОНОВАЯ ЗАДАЧА - УДАЛЕНИЕ ИСТЕКШИХ ТОВАРОВ
# ============================================

async def cleanup_expired_offers():
    """Фоновая задача для удаления истекших предложений"""
    while True:
        try:
            await asyncio.sleep(300)  # Проверяем каждые 5 минут (300 секунд)
            deleted_count = db.delete_expired_offers()
            if deleted_count > 0:
                print(f"🗑 Удалено истекших предложений: {deleted_count}")
        except Exception as e:
            print(f"⚠️ Ошибка при очистке истекших товаров: {e}")

# ============================================
# ЗАПУСК БОТА
# ============================================

async def on_startup():
    """Действия при запуске бота"""
    if USE_WEBHOOK:
        # Устанавливаем webhook
        webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        try:
            await bot.set_webhook(
                url=webhook_url,
                drop_pending_updates=True,
                # Don't restrict allowed_updates to avoid missing types in production
                secret_token=SECRET_TOKEN or None
            )
            print(f"✅ Webhook установлен: {webhook_url}")
        except Exception as e:
            logger.error(f"Failed to set webhook: {e}")
            # Продолжаем запуск HTTP сервера даже если установить webhook не удалось
    else:
        # Удаляем webhook если используем polling
        await bot.delete_webhook(drop_pending_updates=True)
        print("✅ Polling режим активирован")

async def on_shutdown():
    """Действия при остановке бота"""
    await bot.session.close()
    print("👋 Бот остановлен")

# ============== ОТЛАДКА НЕОБРАБОТАННЫХ CALLBACKS ==============

@dp.callback_query()
async def catch_all_callbacks(callback: types.CallbackQuery):
    """Логирование всех callback_data для отладки непойманных обработчиков"""
    data = callback.data or ""
    print(f"[CATCH_ALL] Received callback: {data} from user {callback.from_user.id}")
    try:
        logger.info(f"UNHANDLED callback: {data}")
        print(f"UNHANDLED callback: {data}")
    except Exception:
        pass
    # Закрываем спиннер без алерта
    try:
        await callback.answer()
    except Exception:
        pass

async def main():
    print("✅ Бот успешно запущен!")
    print(f"🔄 Режим: {'Webhook' if USE_WEBHOOK else 'Polling'}")
    print("⚠️ Нажмите Ctrl+C для остановки")
    print("=" * 50)
    
    # Запускаем фоновую задачу очистки
    cleanup_task = asyncio.create_task(cleanup_expired_offers())
    
    if USE_WEBHOOK:
        # Webhook режим (для production на Railway)
        from aiohttp import web
        
        await on_startup()
        
        app = web.Application()
        
        # Webhook endpoint
        async def webhook_handler(request):
            try:
                logger.info(f"Webhook request received from {request.remote}")
                
                # Проверяем секретный токен Telegram при наличии
                if SECRET_TOKEN:
                    hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
                    if hdr != SECRET_TOKEN:
                        logger.warning(f"Invalid secret token from {request.remote}")
                        return web.Response(status=403, text="Forbidden")
                
                # Основная обработка запроса
                update_data = await request.json()
                logger.debug(f"Update data: {update_data}")
                
                telegram_update = types.Update.model_validate(update_data)
                await dp.feed_update(bot, telegram_update)
                
                METRICS["updates_received"] = METRICS.get("updates_received", 0) + 1
                logger.info("Update processed successfully")
                return web.Response(status=200, text="OK")
            except Exception as e:
                logger.error(f"Webhook error: {e}", exc_info=True)
                METRICS["updates_errors"] = METRICS.get("updates_errors", 0) + 1
                # Возвращаем 200 чтобы Telegram не повторял запрос
                return web.Response(status=200, text="OK")
        
        # Health check endpoint
        async def health_check(request):
            return web.json_response({"status": "ok", "bot": "Fudly"})
        async def version_info(request):
            return web.json_response({
                "app": "Fudly",
                "mode": "webhook",
                "port": PORT,
                "use_webhook": USE_WEBHOOK,
                "ts": datetime.now().isoformat(timespec='seconds')
            })
        # Prometheus-style metrics (text/plain) and JSON variant
        def _prometheus_metrics_text():
            help_map = {
                "updates_received": "Total updates received",
                "updates_errors": "Total webhook errors",
                "bookings_created": "Total bookings created",
                "bookings_cancelled": "Total bookings cancelled",
            }
            lines = []
            for key, val in METRICS.items():
                metric = f"fudly_{key}"
                lines.append(f"# HELP {metric} {help_map.get(key, key)}")
                lines.append(f"# TYPE {metric} counter")
                try:
                    v = int(val)
                except Exception:
                    v = 0
                lines.append(f"{metric} {v}")
            return "\n".join(lines) + "\n"

        async def metrics_prom(request):
            text = _prometheus_metrics_text()
            return web.Response(text=text, content_type='text/plain; version=0.0.4; charset=utf-8')

        async def metrics_json(request):
            return web.json_response(METRICS)
        
        # Webhook endpoints (POST + GET for sanity) — register both with and without trailing slash
        path_main = WEBHOOK_PATH if WEBHOOK_PATH.startswith('/') else f'/{WEBHOOK_PATH}'
        path_alt = path_main.rstrip('/') + '/'
        app.router.add_post(path_main, webhook_handler)
        app.router.add_post(path_alt, webhook_handler)
        async def webhook_get(_request):
            return web.Response(text="OK", status=200)
        app.router.add_get(path_main, webhook_get)
        app.router.add_get(path_alt, webhook_get)
        app.router.add_get("/health", health_check)
        app.router.add_get("/version", version_info)
        app.router.add_get("/metrics", metrics_prom)
        app.router.add_get("/metrics.json", metrics_json)
        app.router.add_get("/", health_check)  # Railway health check
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        
        print(f"🌐 Webhook сервер запущен на порту {PORT}")
        
        try:
            await shutdown_event.wait()
        finally:
            cleanup_task.cancel()
            await runner.cleanup()
            await on_shutdown()
    else:
        # Polling режим (для локальной разработки)
        await on_startup()
        
        # Создаём задачу для polling
        polling_task = asyncio.create_task(dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True
        ))
        
        try:
            await shutdown_event.wait()
            print("\n🛑 Завершение по сигналу...")
            polling_task.cancel()
            try:
                await polling_task
            except asyncio.CancelledError:
                pass
        except Exception as e:
            print(f"\n❌ Критическая ошибка: {type(e).__name__}: {e}")
        finally:
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
            await on_shutdown()

# ============================================
# ЗАЩИТА ОТ МНОЖЕСТВЕННОГО ЗАПУСКА
# ============================================

def is_bot_already_running(port=8444):
    """Проверяет, не запущен ли уже бот на этом порту"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', port))
        sock.close()
        return False
    except OSError:
        print(f"🛑 ОШИБКА: Бот уже запущен на порту {port}!")
        print("⚠️ Остановите другой экземпляр перед запуском нового.")
        return True

# Глобальная переменная для graceful shutdown
shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    """Обработчик сигнала завершения (Ctrl+C)"""
    print("\n🛑 Получен сигнал завершения...")
    shutdown_event.set()

if __name__ == "__main__":
    # Проверяем, не запущен ли бот уже
    if is_bot_already_running():
        print("❌ Завершение работы дубликата...")
        sys.exit(1)
    
    print("=" * 50)
    print("🚀 Запуск бота Fudly (Production Optimized)...")
    print("=" * 50)
    print(f"📊 База данных: {db.db_name}")
    if ADMIN_ID > 0:
        print(f"👑 Главный админ: {ADMIN_ID}")
    print(f"🔒 Порт блокировки: 8444")
    print(f"🌍 Языки: Русский, Узбекский")
    print(f"📸 Поддержка фото: Да")
    print(f"⚡ Оптимизация: Пулинг соединений, кэширование, безопасность")
    print("=" * 50)
    
    # Start background tasks for cleanup and maintenance
    if PRODUCTION_FEATURES:
        logger.info("Starting background tasks...")
        start_background_tasks(db)
        print("✅ Background tasks started")
    else:
        print("⚠️ Running in basic mode (production features disabled)")
    
    # Устанавливаем обработчик сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("Bot starting...")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        print("\n👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Bot crashed: {str(e)}")
        print(f"\n❌ Ошибка: {e}")
    finally:
        logger.info("Bot shutdown complete")

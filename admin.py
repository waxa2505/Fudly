"""
Admin panel handlers
Note: This module contains the main admin handlers. Additional admin handlers  
remain in bot.py and can be migrated here incrementally.
"""
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

router = Router()


def setup(dp_or_router, db, get_text, admin_menu):
    """Setup admin handlers with dependencies"""
    from handlers.common import get_uzb_time
    
    @dp_or_router.message(Command("admin"))
    async def cmd_admin(message: types.Message):
        lang = db.get_user_language(message.from_user.id)
        
        if not db.is_admin(message.from_user.id):
            await message.answer(get_text(lang, 'no_admin_access'))
            return
        
        await message.answer(
            "👑 <b>Админ-панель Fudly</b>\n\n"
            "Добро пожаловать! Выберите раздел:",
            parse_mode="HTML",
            reply_markup=admin_menu()
        )

    @dp_or_router.message(F.text == "📊 Dashboard")
    async def admin_dashboard(message: types.Message):
        """Main panel with general statistics and quick actions"""
        if not db.is_admin(message.from_user.id):
            return
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # General statistics
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE role = "seller"')
        sellers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE role = "customer"')
        customers = cursor.fetchone()[0]
        
        # Stores
        cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "active"')
        active_stores = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM stores WHERE status = "pending"')
        pending_stores = cursor.fetchone()[0]
        
        # Offers
        cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "active"')
        active_offers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM offers WHERE status = "inactive"')
        inactive_offers = cursor.fetchone()[0]
        
        # Bookings
        cursor.execute('SELECT COUNT(*) FROM bookings')
        total_bookings = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = "pending"')
        pending_bookings = cursor.fetchone()[0]
        
        # Today's statistics (Uzbek time)
        today = get_uzb_time().strftime('%Y-%m-%d')
        
        cursor.execute('SELECT COUNT(*) FROM bookings WHERE DATE(created_at) = ?', (today,))
        today_bookings = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT SUM(o.discount_price * b.quantity)
            FROM bookings b
            JOIN offers o ON b.offer_id = o.offer_id
            WHERE DATE(b.created_at) = ? AND b.status != 'cancelled'
        ''', (today,))
        today_revenue = cursor.fetchone()[0] or 0
        
        # New users today
        cursor.execute('''
            SELECT COUNT(*) FROM users 
            WHERE DATE(created_at) = ?
        ''', (today,))
        today_users = cursor.fetchone()[0]
        
        conn.close()
        
        # Format message
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
        
        # Inline buttons for quick actions
        kb = InlineKeyboardBuilder()
        
        if pending_stores > 0:
            kb.button(text=f"⏳ Модерация ({pending_stores})", callback_data="admin_moderation")
        
        kb.button(text="📊 Детальная статистика", callback_data="admin_detailed_stats")
        kb.button(text="🔄 Обновить", callback_data="admin_refresh_dashboard")
        kb.adjust(1)
        
        await message.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

    @dp_or_router.message(F.text == "🔙 Выход")
    async def admin_exit(message: types.Message):
        """Exit admin panel"""
        if not db.is_admin(message.from_user.id):
            return
        
        lang = db.get_user_language(message.from_user.id)
        user = db.get_user(message.from_user.id)
        
        # Import here to avoid circular dependencies
        from keyboards import main_menu_customer, main_menu_seller
        
        # Return to appropriate main menu based on user role
        menu = main_menu_seller(lang) if user and user[6] == "seller" else main_menu_customer(lang)
        
        await message.answer(
            "👋 Выход из админ-панели",
            reply_markup=menu
        )

"""
Common utilities, state classes, and middleware
"""
from aiogram.fsm.state import State, StatesGroup
from aiogram import BaseMiddleware
from aiogram.types import Update
from typing import Callable, Dict, Any, Awaitable
from datetime import timezone, timedelta, datetime

# In-memory per-session view mode override: {'seller'|'customer'}
user_view_mode = {}

# Uzbek city names mapping to Russian
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

# Uzbek timezone (UTC+5)
UZB_TZ = timezone(timedelta(hours=5))


def normalize_city(city: str) -> str:
    """Convert city name to Russian format for database search"""
    return CITY_UZ_TO_RU.get(city, city)


def get_uzb_time():
    """Get current time in Uzbek timezone (UTC+5)"""
    return datetime.now(UZB_TZ)


def has_approved_store(user_id: int, db) -> bool:
    """Check if user has an approved store"""
    stores = db.get_user_stores(user_id)
    # stores: [0]store_id, [1]owner_id, [2]name, [3]city, [4]address, [5]description, 
    #         [6]category, [7]phone, [8]status, [9]rejection_reason, [10]created_at
    return any(store[8] == "active" for store in stores if len(store) > 8)


def get_appropriate_menu(user_id: int, lang: str, db, main_menu_seller, main_menu_customer):
    """Return appropriate menu for user based on their store approval status"""
    user = db.get_user(user_id)
    if not user:
        return main_menu_customer(lang)
    
    role = user[6] if len(user) > 6 else "customer"
    
    # If partner - check for approved store
    if role == "seller":
        if has_approved_store(user_id, db):
            return main_menu_seller(lang)
        else:
            # No approved store - show customer menu
            return main_menu_customer(lang)
    
    return main_menu_customer(lang)


# ============== FSM STATES ==============

class Registration(StatesGroup):
    phone = State()
    city = State()

class RegisterStore(StatesGroup):
    city = State()
    category = State()
    name = State()
    address = State()
    description = State()
    phone = State()

class CreateOffer(StatesGroup):
    store = State()
    title = State()
    photo = State()
    original_price = State()
    discount_price = State()
    quantity = State()
    available_until = State()
    category = State()
    unit = State()

class BulkCreate(StatesGroup):
    store = State()
    count = State()
    titles = State()
    photos = State()
    original_prices = State()
    discount_prices = State()
    quantities = State()
    available_untils = State()
    categories = State()
    units = State()

class ChangeCity(StatesGroup):
    new_city = State()

class EditOffer(StatesGroup):
    offer_id = State()
    field = State()
    value = State()

class ConfirmOrder(StatesGroup):
    offer_id = State()

class BookOffer(StatesGroup):
    quantity = State()


# ============== MIDDLEWARE: REGISTRATION CHECK ==============

class RegistrationCheckMiddleware(BaseMiddleware):
    """Check that user is registered (has phone number) before any action"""
    
    def __init__(self, db, get_text_func, phone_request_keyboard_func):
        self.db = db
        self.get_text = get_text_func
        self.phone_request_keyboard = phone_request_keyboard_func
        super().__init__()
    
    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any]
    ) -> Any:
        # Determine user_id from different event types
        user_id = None
        if event.message:
            user_id = event.message.from_user.id
        elif event.callback_query:
            user_id = event.callback_query.from_user.id
        
        if not user_id:
            return await handler(event, data)
        
        # Commands that are always allowed (for registration process)
        allowed_commands = ['/start', '/help']
        allowed_callbacks = ['lang_ru', 'lang_uz']  # Language selection during registration
        
        # Check if this is an allowed command
        if event.message and event.message.text:
            if any(event.message.text.startswith(cmd) for cmd in allowed_commands):
                return await handler(event, data)
            # Allow sending contact (phone number)
            if event.message.contact:
                return await handler(event, data)
        
        # Allow language selection callbacks
        if event.callback_query and event.callback_query.data in allowed_callbacks:
            return await handler(event, data)
        
        # Check FSM state — if user is in registration process, allow
        state = data.get('state')
        if state:
            current_state = await state.get_state()
            if current_state and current_state.startswith('Registration:'):
                # User is in registration process — allow
                return await handler(event, data)
        
        # Check user registration
        user = self.db.get_user(user_id)
        if not user or not user[3]:  # user[3] is phone
            lang = self.db.get_user_language(user_id) if user else 'ru'
            
            # If this is a message
            if event.message:
                await event.message.answer(
                    self.get_text(lang, 'registration_required'),
                    parse_mode="HTML",
                    reply_markup=self.phone_request_keyboard(lang)
                )
            # If this is a callback
            elif event.callback_query:
                await event.callback_query.answer(
                    self.get_text(lang, 'registration_required'),
                    show_alert=True
                )
            return  # Block further processing
        
        # User is registered — continue
        return await handler(event, data)

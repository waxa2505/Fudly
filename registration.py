"""
User registration handlers (phone and city collection)
"""
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext

from handlers.common import Registration

router = Router()


def setup(dp_or_router, db, get_text, get_cities, city_keyboard, main_menu_customer, 
          validator, rate_limiter, logger, secure_user_input):
    """Setup registration handlers with dependencies"""
    
    @dp_or_router.message(Registration.phone, F.contact)
    async def process_phone(message: types.Message, state: FSMContext):
        # User must be created when choosing language
        lang = db.get_user_language(message.from_user.id)
        phone = message.contact.phone_number
        
        # Update phone number
        db.update_user_phone(message.from_user.id, phone)
        
        await message.answer(
            get_text(lang, 'choose_city'),
            parse_mode="HTML",
            reply_markup=city_keyboard(lang, allow_cancel=False)
        )
        await state.set_state(Registration.city)

    @dp_or_router.message(Registration.city)
    @secure_user_input
    async def process_city(message: types.Message, state: FSMContext):
        lang = db.get_user_language(message.from_user.id)
        
        # Rate limiting check
        try:
            if not rate_limiter.is_allowed(message.from_user.id, 'city_selection', max_requests=5, window_seconds=60):
                await message.answer(get_text(lang, 'rate_limit_exceeded'))
                return
        except Exception as e:
            logger.warning(f"Rate limiter error: {e}")
        
        cities = get_cities(lang)
        city_text = validator.sanitize_text(message.text.replace("📍 ", "").strip())
        
        # Validate city input
        if not validator.validate_city(city_text):
            await message.answer(get_text(lang, 'invalid_city'))
            return
        
        if city_text in cities:
            db.update_user_city(message.from_user.id, city_text)
            await state.clear()
            await message.answer(
                get_text(lang, 'city_changed', city=city_text),
                reply_markup=main_menu_customer(lang)
            )

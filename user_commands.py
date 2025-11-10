"""
User command handlers (start, language selection, city selection, cancel actions)
"""
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

router = Router()


def setup(dp_or_router, db, get_text, get_cities, city_keyboard, language_keyboard,
          phone_request_keyboard, main_menu_seller, main_menu_customer):
    """Setup user command handlers with dependencies"""
    from handlers.common import Registration, user_view_mode, has_approved_store
    
    @dp_or_router.message(F.text == "Мой город")
    async def my_city(message: types.Message, state: FSMContext = None):
        user_id = message.from_user.id
        lang = db.get_user_language(user_id)
        user = db.get_user(user_id)
        current_city = user[4] if user and len(user) > 4 else None
        if not current_city:
            current_city = get_cities(lang)[0]
        
        # Get city statistics
        stats_text = ""
        try:
            stores_count = len(db.get_stores_by_city(current_city))
            offers_count = len(db.get_active_offers(city=current_city))
            stats_text = f"\n\n📊 В вашем городе:\n🏪 Магазинов: {stores_count}\n🍽 Предложений: {offers_count}"
        except:
            pass
        
        # Create inline keyboard with buttons
        builder = InlineKeyboardBuilder()
        builder.button(
            text="✏️ Изменить город" if lang == 'ru' else "✏️ Shaharni o'zgartirish",
            callback_data="change_city"
        )
        builder.button(
            text="◀️ Назад" if lang == 'ru' else "◀️ Orqaga",
            callback_data="back_to_menu"
        )
        builder.adjust(1)
        
        await message.answer(
            f"{get_text(lang, 'your_city')}: {current_city}{stats_text}",
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )

    @dp_or_router.callback_query(F.data == "change_city")
    async def show_city_selection(callback: types.CallbackQuery, state: FSMContext):
        """Show list of cities for selection"""
        lang = db.get_user_language(callback.from_user.id)
        await callback.message.edit_text(
            get_text(lang, 'choose_city'),
            reply_markup=city_keyboard(lang)
        )
        await callback.answer()

    @dp_or_router.callback_query(F.data == "back_to_menu")
    async def back_to_main_menu(callback: types.CallbackQuery):
        """Return to main menu"""
        lang = db.get_user_language(callback.from_user.id)
        user = db.get_user(callback.from_user.id)
        menu = main_menu_seller(lang) if user and user[6] == "seller" else main_menu_customer(lang)
        
        await callback.message.delete()
        await callback.message.answer(
            get_text(lang, 'main_menu') if 'main_menu' in dir() else "Главное меню",
            reply_markup=menu
        )
        await callback.answer()

    @dp_or_router.message(F.text.in_(get_cities('ru') + get_cities('uz')))
    async def change_city(message: types.Message, state: FSMContext = None):
        """Quick city change handler (without FSM state)"""
        user_id = message.from_user.id
        lang = db.get_user_language(user_id)
        user = db.get_user(user_id)
        
        # IMPORTANT: Check current FSM state
        # If user is in registration process (store or self), skip
        if state:
            current_state = await state.get_state()
            if current_state and (current_state.startswith('RegisterStore:') or current_state.startswith('Registration:')):
                # User is in registration process — don't touch, let corresponding handler process
                return
        
        new_city = message.text
        
        # Save new city
        db.update_user_city(user_id, new_city)
        
        # Get updated main menu
        menu = main_menu_seller(lang) if user and user[6] == "seller" else main_menu_customer(lang)
        
        await message.answer(
            f"✅ {get_text(lang, 'city_changed', city=new_city)}\n\n"
            f"Теперь вы будете видеть предложения из города {new_city}",
            reply_markup=menu,
            parse_mode="HTML"
        )

    @dp_or_router.message(Command("start"))
    async def cmd_start(message: types.Message, state: FSMContext):
        user = db.get_user(message.from_user.id)
        
        if not user:
            # New user - show language selection
            # DO NOT create user until we get phone number!
            await message.answer(
                get_text('ru', 'choose_language'),
                reply_markup=language_keyboard()
            )
            return
        
        lang = db.get_user_language(message.from_user.id)
        
        # Check phone
        if not user[3]:
            await message.answer(
                get_text(lang, 'welcome', name=message.from_user.first_name),
                parse_mode="HTML",
                reply_markup=phone_request_keyboard(lang)
            )
            await state.set_state(Registration.phone)
            return
        
        # Check city
        if not user[4]:
            await message.answer(
                get_text(lang, 'choose_city'),
                parse_mode="HTML",
                reply_markup=city_keyboard(lang, allow_cancel=False)
            )
            await state.set_state(Registration.city)
            return
        
        # Welcome message
        menu = main_menu_seller(lang) if user[6] == "seller" else main_menu_customer(lang)
        await message.answer(
            get_text(lang, 'welcome_back', name=message.from_user.first_name, city=user[4]),
            parse_mode="HTML",
            reply_markup=menu
        )

    @dp_or_router.callback_query(F.data.startswith("lang_"))
    async def choose_language(callback: types.CallbackQuery, state: FSMContext):
        lang = callback.data.split("_")[1]
        
        # Show menu after language selection
        user = db.get_user(callback.from_user.id)
        
        # CHECK: if user is not in DB (new user)
        if not user:
            # Create new user WITH selected language
            db.add_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
            db.update_user_language(callback.from_user.id, lang)
            await callback.message.edit_text(get_text(lang, 'language_changed'))
            await callback.message.answer(
                get_text(lang, 'welcome', name=callback.from_user.first_name),
                parse_mode="HTML",
                reply_markup=phone_request_keyboard(lang)
            )
            await state.set_state(Registration.phone)
            return
        
        # If user already exists — just update language
        db.update_user_language(callback.from_user.id, lang)
        await callback.message.edit_text(get_text(lang, 'language_changed'))
        
        # If no phone - request it
        if not user[3]:
            await callback.message.answer(
                get_text(lang, 'welcome', name=callback.from_user.first_name),
                parse_mode="HTML",
                reply_markup=phone_request_keyboard(lang)
            )
            await state.set_state(Registration.phone)
            return
        
        # If no city - request it
        if not user[4]:
            await callback.message.answer(
                get_text(lang, 'choose_city'),
                parse_mode="HTML",
                reply_markup=city_keyboard(lang, allow_cancel=False)
            )
            await state.set_state(Registration.city)
            return
        
        # Show main menu
        menu = main_menu_seller(lang) if user[6] == "seller" else main_menu_customer(lang)
        await callback.message.answer(
            get_text(lang, 'welcome_back', name=callback.from_user.first_name, city=user[4]),
            parse_mode="HTML",
            reply_markup=menu
        )

    @dp_or_router.message(F.text.contains("Отмена") | F.text.contains("Bekor qilish"))
    async def cancel_action(message: types.Message, state: FSMContext):
        lang = db.get_user_language(message.from_user.id)
        current_state = await state.get_state()
        
        # BLOCK cancellation of mandatory registration
        if current_state in ['Registration:phone', 'Registration:city']:
            user = db.get_user(message.from_user.id)
            # If no phone number — registration is mandatory, cancellation prohibited
            if not user or not user[3]:
                await message.answer(
                    "❌ Регистрация обязательна для использования бота.\n\n"
                    "📱 Пожалуйста, поделитесь номером телефона.",
                    reply_markup=phone_request_keyboard(lang)
                )
                return
        
        # For all other states — allow cancellation
        await state.clear()

        # Map state group to preferred menu context
        seller_groups = {"RegisterStore", "CreateOffer", "BulkCreate", "ConfirmOrder"}
        customer_groups = {"Registration", "BookOffer", "ChangeCity"}

        preferred_menu = None
        if current_state:
            try:
                state_group = str(current_state).split(":", 1)[0]
                if state_group in seller_groups:
                    preferred_menu = "seller"
                elif state_group in customer_groups:
                    preferred_menu = "customer"
            except Exception:
                preferred_menu = None

        user = db.get_user(message.from_user.id)
        role = user[6] if user and len(user) > 6 else "customer"
        
        # CRITICAL: When cancelling RegisterStore ALWAYS return to customer menu
        # because user does NOT YET have an approved store
        if current_state and str(current_state).startswith("RegisterStore"):
            # Cancel store registration - return to customer menu
            await message.answer(
                get_text(lang, 'operation_cancelled'),
                reply_markup=main_menu_customer(lang)
            )
            return
        
        # IMPORTANT: Check for approved store for partners
        if role == "seller":
            # Use ready function has_approved_store for checking
            if not has_approved_store(message.from_user.id, db):
                # No approved store — show customer menu
                role = "customer"
                preferred_menu = "customer"
        
        # View mode override has priority if set
        view_override = user_view_mode.get(message.from_user.id)
        target = preferred_menu or view_override or ("seller" if role == "seller" else "customer")
        menu = main_menu_seller(lang) if target == "seller" else main_menu_customer(lang)

        await message.answer(
            get_text(lang, 'operation_cancelled'),
            reply_markup=menu
        )

    @dp_or_router.callback_query(F.data == "cancel_offer")
    async def cancel_offer_callback(callback: types.CallbackQuery, state: FSMContext):
        """Handler for offer creation cancel button"""
        lang = db.get_user_language(callback.from_user.id)
        await state.clear()
        
        await callback.message.edit_text(
            f"❌ {'Создание товара отменено' if lang == 'ru' else 'Mahsulot yaratish bekor qilindi'}",
            parse_mode="HTML"
        )
        
        await callback.message.answer(
            get_text(lang, 'operation_cancelled'),
            reply_markup=main_menu_seller(lang)
        )
        
        await callback.answer()

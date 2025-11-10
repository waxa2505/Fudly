from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from localization import get_text, LANGUAGES

# Список городов Узбекистана
CITIES_RU = ["Ташкент", "Самарканд", "Бухара", "Андижан", "Наманган", "Фергана", "Хива", "Нукус"]
CITIES_UZ = ["Toshkent", "Samarqand", "Buxoro", "Andijon", "Namangan", "Farg'ona", "Xiva", "Nukus"]

# Категории заведений
CATEGORIES_RU = ["Ресторан", "Кафе", "Пекарня", "Супермаркет", "Кондитерская", "Фастфуд"]
CATEGORIES_UZ = ["Restoran", "Kafe", "Nonvoyxona", "Supermarket", "Qandolatxona", "Fastfud"]

def get_cities(lang: str) -> list:
    """Получить список городов на нужном языке"""
    return CITIES_UZ if lang == 'uz' else CITIES_RU

def get_categories(lang: str) -> list:
    """Получить список категорий на нужном языке"""
    return CATEGORIES_UZ if lang == 'uz' else CATEGORIES_RU

# ============== ВЫБОР ЯЗЫКА ==============

def language_keyboard():
    """Клавиатура выбора языка"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🇷🇺 Русский", callback_data="lang_ru")
    builder.button(text="🇺🇿 O'zbekcha", callback_data="lang_uz")
    builder.adjust(2)
    return builder.as_markup()

# ============== ОСНОВНЫЕ МЕНЮ ==============

def main_menu_customer(lang: str = 'ru'):
    """Главное меню для покупателя"""
    builder = ReplyKeyboardBuilder()
    builder.button(text=get_text(lang, 'available_offers'))
    builder.button(text=get_text(lang, 'stores'))
    builder.button(text=get_text(lang, 'my_bookings'))
    builder.button(text=get_text(lang, 'my_city'))
    builder.button(text=get_text(lang, 'profile'))
    builder.button(text=get_text(lang, 'become_partner'))
    builder.adjust(2, 2, 2)
    return builder.as_markup(resize_keyboard=True)

def main_menu_seller(lang: str = 'ru'):
    """Упрощённое меню партнёра: Добавить, Товары, Заказы, Сегодня, Профиль"""
    builder = ReplyKeyboardBuilder()
    builder.button(text=get_text(lang, 'add_item'))
    builder.button(text=get_text(lang, 'my_items'))
    builder.button(text=get_text(lang, 'orders'))
    builder.button(text=get_text(lang, 'today_stats'))
    builder.button(text=get_text(lang, 'profile'))
    builder.adjust(2, 2, 1)
    return builder.as_markup(resize_keyboard=True)

# ============== ВЫБОР ГОРОДА И КАТЕГОРИИ ==============

def city_keyboard(lang: str = 'ru', allow_cancel: bool = True):
    """Клавиатура выбора города
    
    Args:
        lang: Язык интерфейса
        allow_cancel: Показывать ли кнопку отмены (False для обязательной регистрации)
    """
    cities = get_cities(lang)
    builder = ReplyKeyboardBuilder()
    for city in cities:
        builder.button(text=f"📍 {city}")
    
    if allow_cancel:
        builder.button(text=f"❌ {get_text(lang, 'cancel')}")
        builder.adjust(2, 2, 2, 2, 1)
    else:
        builder.adjust(2, 2, 2, 2)
    
    return builder.as_markup(resize_keyboard=True)

def category_keyboard(lang: str = 'ru'):
    """Клавиатура выбора категории"""
    categories = get_categories(lang)
    builder = ReplyKeyboardBuilder()
    for cat in categories:
        builder.button(text=f"🏷 {cat}")
    builder.button(text=f"❌ {get_text(lang, 'cancel')}")
    builder.adjust(2, 2, 2, 1)
    return builder.as_markup(resize_keyboard=True)

# ============== INLINE КЛАВИАТУРЫ ==============

def offer_keyboard(offer_id: int, lang: str = 'ru'):
    """Клавиатура для предложения"""
    builder = InlineKeyboardBuilder()
    builder.button(text=get_text(lang, 'book'), callback_data=f"book_{offer_id}")
    builder.button(text=get_text(lang, 'details'), callback_data=f"details_{offer_id}")
    builder.adjust(1)
    return builder.as_markup()

def offer_manage_keyboard(offer_id: int, lang: str = 'ru'):
    """Клавиатура управления предложением"""
    builder = InlineKeyboardBuilder()
    builder.button(text=get_text(lang, 'duplicate'), callback_data=f"duplicate_{offer_id}")
    builder.button(text=get_text(lang, 'delete'), callback_data=f"delete_offer_{offer_id}")
    builder.adjust(2)
    return builder.as_markup()

def booking_keyboard(booking_id: int, lang: str = 'ru'):
    """Клавиатура для бронирования"""
    builder = InlineKeyboardBuilder()
    builder.button(text=get_text(lang, 'cancel_booking'), callback_data=f"cancel_booking_{booking_id}")
    return builder.as_markup()

def rate_keyboard(booking_id: int):
    """Клавиатура для оценки"""
    builder = InlineKeyboardBuilder()
    builder.button(text="⭐⭐⭐⭐⭐", callback_data=f"rate_{booking_id}_5")
    builder.button(text="⭐⭐⭐⭐", callback_data=f"rate_{booking_id}_4")
    builder.button(text="⭐⭐⭐", callback_data=f"rate_{booking_id}_3")
    builder.button(text="⭐⭐", callback_data=f"rate_{booking_id}_2")
    builder.button(text="⭐", callback_data=f"rate_{booking_id}_1")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def stores_list_keyboard(stores, lang: str = 'ru'):
    """Клавиатура списка магазинов
    
    Args:
        stores: Список кортежей магазинов. Структура зависит от запроса:
               - Если из get_stores_by_category: [0]=store_id, [1]=name, [2]=address, [3]=category, [4]=city
               - Если из get_store/get_user_stores: [0]=store_id, [1]=owner_id, [2]=name, [3]=city, [4]=address, ...
    """
    builder = InlineKeyboardBuilder()
    for store in stores[:10]:
        # Безопасная обработка разных структур store
        if len(store) >= 3:
            # Определяем структуру по позиции name
            # Если store[2] похож на адрес (содержит длинный текст) и store[1] короткий - значит [1]=name, [2]=address
            # Если store[2] короткий - значит [2]=name (для get_stores_by_category)
            if len(store) >= 5:
                # Структура get_stores_by_category: [0]=store_id, [1]=name, [2]=address, [3]=category, [4]=city
                store_name = store[1] if len(store) > 1 else "Магазин"
                store_address = store[2] if len(store) > 2 else ""
            else:
                # Другая структура (get_user_stores): [0]=store_id, [1]=owner_id, [2]=name, ...
                store_name = store[2] if len(store) > 2 else "Магазин"
                store_address = store[4] if len(store) > 4 else ""
        else:
            store_name = "Магазин"
            store_address = ""
        
        store_id = store[0] if len(store) > 0 else 0
        
        if store_address:
            button_text = f"🏪 {store_name} - 📍 {store_address}"
        else:
            button_text = f"🏪 {store_name}"
        
        builder.button(
            text=button_text, 
            callback_data=f"filter_store_{store_id}"
        )
    builder.button(text=f"🔄 {get_text(lang, 'available_offers')}", callback_data="filter_all")
    builder.adjust(1)
    return builder.as_markup()

def phone_request_keyboard(lang: str = 'ru'):
    """Клавиатура запроса телефона (без кнопки отмены — регистрация обязательна)"""
    builder = ReplyKeyboardBuilder()
    builder.button(text=f"📱 {get_text(lang, 'share_phone')}", request_contact=True)
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)

def cancel_keyboard(lang: str = 'ru'):
    """Клавиатура отмены"""
    builder = ReplyKeyboardBuilder()
    builder.button(text=f"❌ {get_text(lang, 'cancel')}")

    return builder.as_markup(resize_keyboard=True)

# ============== ФИЛЬТРЫ БРОНИРОВАНИЙ ==============
def booking_filters_keyboard(lang: str = 'ru', active: int = 0, completed: int = 0, cancelled: int = 0):
    """Клавиатура фильтров для бронирований"""
    builder = InlineKeyboardBuilder()
    builder.button(text=f"🟢 Активные ({active})", callback_data="bookings_active")
    builder.button(text=f"✅ Завершенные ({completed})", callback_data="bookings_completed")
    builder.button(text=f"❌ Отмененные ({cancelled})", callback_data="bookings_cancelled")
    builder.adjust(1)
    return builder.as_markup()

# ============== АДМИН ПАНЕЛЬ ==============

def admin_menu(lang: str = 'ru'):
    """Компактное меню администратора с улучшенной группировкой"""
    builder = ReplyKeyboardBuilder()
    builder.button(text="📊 Dashboard")
    builder.button(text="👥 Пользователи")
    builder.button(text="🏪 Магазины")
    builder.button(text="📦 Товары")
    builder.button(text="📋 Бронирования")
    builder.button(text="📈 Аналитика")
    builder.button(text="📢 Рассылка")
    builder.button(text="⚙️ Настройки")
    builder.button(text="🔙 Выход")
    builder.adjust(2, 2, 2, 2, 1)
    return builder.as_markup(resize_keyboard=True)

def moderation_keyboard(store_id: int):
    """Кнопки модерации магазина"""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Одобрить", callback_data=f"approve_store_{store_id}")
    builder.button(text="❌ Отклонить", callback_data=f"reject_store_{store_id}")
    builder.adjust(2)
    return builder.as_markup()

def settings_keyboard(notifications_enabled: bool, lang: str = 'ru', role: str | None = None):
    """Клавиатура настроек профиля с учётом роли пользователя.
    
    Args:
        notifications_enabled: Включены ли уведомления
        lang: Язык интерфейса
        role: Роль пользователя ('seller' или 'customer')
    """
    from localization import get_text

    builder = InlineKeyboardBuilder()
    
    # Для партнёра показываем переключение в режим покупателя
    if role == 'seller':
        builder.button(text="🔄 Режим покупателя" if lang == 'ru' else "🔄 Xaridor rejimi", callback_data="switch_to_customer")
    else:
        # Для покупателя показываем "Стать партнёром"
        builder.button(text=get_text(lang, 'become_partner'), callback_data="become_partner_cb")
    
    # Уведомления
    notif_text = get_text(lang, 'notifications_enabled') if notifications_enabled else get_text(lang, 'notifications_disabled')
    builder.button(text=notif_text, callback_data="toggle_notifications")
    
    # Смена города
    builder.button(text="🌆 Сменить город" if lang == 'ru' else "🌆 Shaharni o'zgartirish", callback_data="profile_change_city")
    
    # Смена языка
    builder.button(text=get_text(lang, 'change_language'), callback_data="change_language")
    
    # Удаление аккаунта
    builder.button(text=get_text(lang, 'delete_account'), callback_data="delete_account")
    
    builder.adjust(1, 1, 1, 1, 1)  # Все кнопки в столбик
    return builder.as_markup()

def store_keyboard(store_id: int):
    """Кнопки управления магазином"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data=f"store_stats_{store_id}")
    builder.button(text="📋 Предложения", callback_data=f"show_offers_{store_id}")
    builder.adjust(2)
    return builder.as_markup()

# ============== ЕДИНИЦЫ ИЗМЕРЕНИЯ ==============

def units_keyboard(lang: str = 'ru'):
    """Клавиатура выбора единиц измерения"""
    builder = ReplyKeyboardBuilder()
    units = ['шт', 'кг', 'г', 'л', 'мл', 'упак', 'м', 'см']
    for unit in units:
        builder.button(text=unit)
    builder.adjust(4, 4)  # 4 кнопки в первом ряду, 4 во втором
    return builder.as_markup(resize_keyboard=True)

# ============== КАТЕГОРИИ ТОВАРОВ ==============

def product_categories_keyboard(lang: str = 'ru'):
    """Клавиатура выбора категорий товаров для супермаркетов"""
    builder = ReplyKeyboardBuilder()
    
    categories_ru = [
        '🍞 Хлеб и выпечка', '🥛 Молочные продукты', '🥩 Мясо и птица', 
        '🐟 Рыба и морепродукты', '🥬 Овощи', '🍎 Фрукты и ягоды',
        '🧀 Сыры', '🥚 Яйца', '🍚 Крупы и макароны', '🥫 Консервы',
        '🍫 Кондитерские изделия', '🍪 Печенье и снэки', '☕ Чай и кофе', 
        '🥤 Напитки', '🧴 Бытовая химия', '🧼 Гигиена', '🏠 Для дома', '🎯 Другое'
    ]
    
    categories_uz = [
        '🍞 Non va pishiriq', '🥛 Sut mahsulotlari', '🥩 Go\'sht va parrandalar', 
        '🐟 Baliq va dengiz mahsulotlari', '🥬 Sabzavotlar', '🍎 Mevalar va rezavorlar',
        '🧀 Pishloqlar', '🥚 Tuxum', '🍚 Yorma va makaron', '🥫 Konservalar',
        '🍫 Qandolat mahsulotlari', '🍪 Pechene va sneklar', '☕ Choy va qahva', 
        '🥤 Ichimliklar', '🧴 Maishiy kimyo', '🧼 Gigiyena', '🏠 Uy uchun', '🎯 Boshqa'
    ]
    
    categories = categories_uz if lang == 'uz' else categories_ru
    
    for category in categories:
        builder.button(text=category)
    
    builder.adjust(2, 2, 2, 2, 2, 2, 2, 2, 2)  # По 2 кнопки в ряду
    return builder.as_markup(resize_keyboard=True)

def store_category_selection(lang: str = 'ru'):
    """Inline клавиатура для выбора категории заведения"""
    builder = InlineKeyboardBuilder()
    
    categories = get_categories(lang)
    
    for i, category in enumerate(categories):
        builder.button(text=category, callback_data=f"cat_{i}")
    
    builder.adjust(2)  # 2 кнопки в ряду
    return builder.as_markup()

def offers_category_filter(lang: str = 'ru'):
    """Inline клавиатура с фильтрами по категориям для предложений"""
    builder = InlineKeyboardBuilder()
    
    categories = get_categories(lang)
    
    # Кнопка "Все предложения"
    builder.button(text="🔥 Все" if lang == 'ru' else "🔥 Hammasi", callback_data="offers_all")
    
    # Категории для фильтрации
    for i, category in enumerate(categories):
        # Убираем эмодзи для компактности, оставляем только текст
        cat_text = category.split()[0] if len(category.split()) > 0 else category
        builder.button(text=cat_text, callback_data=f"offers_cat_{i}")
    
    builder.adjust(2, 2, 2, 1)  # 2-2-2-1 кнопок в рядах
    return builder.as_markup()

def stores_category_selection(lang: str = 'ru', counts: dict = None):
    """Inline клавиатура для выбора категории магазинов с количеством"""
    builder = InlineKeyboardBuilder()
    
    categories = get_categories(lang)
    
    # Кнопка "Топ по рейтингу"
    builder.button(text="⭐ Топ по рейтингу" if lang == 'ru' else "⭐ Top reytingli", 
                   callback_data="stores_top")
    
    # Категории с количеством магазинов
    for i, category in enumerate(categories):
        count = counts.get(category, 0) if counts else 0
        button_text = f"{category} ({count})" if count > 0 else category
        builder.button(text=button_text, callback_data=f"stores_cat_{i}")
    
    builder.adjust(1, 2, 2, 2)  # 1 кнопка топ, потом по 2
    return builder.as_markup()

def store_selection(stores, lang: str = 'ru', cat_index: int | None = None, offset: int = 0, page_size: int = 10):
    """Inline клавиатура для выбора магазина с пагинацией."""
    builder = InlineKeyboardBuilder()

    total = len(stores)
    start = max(0, offset)
    end = min(total, start + page_size)

    for store in stores[start:end]:
        # store tuple from get_stores_by_category: [0]=store_id, [1]=name, [2]=address, [3]=category, [4]=city
        if not store or len(store) < 2:
            continue  # Пропускаем некорректные записи
        
        store_id = store[0] if len(store) > 0 else 0
        store_name = store[1] if len(store) > 1 else "Магазин"
        city = store[4] if len(store) > 4 else ''
        
        # Формируем текст кнопки с обрезкой длинных названий
        if city:
            button_text = f"{store_name} ({city})"
        else:
            button_text = store_name
        
        # Обрезаем длинный текст
        if len(button_text) > 50:
            button_text = button_text[:47] + "..."
        
        builder.button(text=button_text, callback_data=f"store_{store_id}")

    # Навигация по страницам
    nav_row = []
    if start > 0 and cat_index is not None:
        prev_off = max(0, start - page_size)
        nav_row.append(("⬅️", f"stores_prev_{cat_index}_{prev_off}"))
    if end < total and cat_index is not None:
        next_off = end
        nav_row.append(("➡️", f"stores_next_{cat_index}_{next_off}"))
    for txt, cb in nav_row:
        builder.button(text=txt, callback_data=cb)

    builder.button(text=get_text(lang, 'back'), callback_data="back_to_categories")
    builder.adjust(1)
    return builder.as_markup()

def offer_selection(offers, lang: str = 'ru', store_id: int | None = None, offset: int = 0, page_size: int = 10):
    """Inline клавиатура для выбора предложения с простейшей пагинацией."""
    builder = InlineKeyboardBuilder()

    total = len(offers)
    start = max(0, offset)
    end = min(total, start + page_size)

    for offer in offers[start:end]:
        if not offer or len(offer) < 3:
            continue  # Пропускаем некорректные записи
        
        # Защита от деления на ноль и ошибок индексации
        try:
            offer_id = offer[0] if len(offer) > 0 else 0
            offer_title = offer[2] if len(offer) > 2 else "Предложение"
            original_price = offer[4] if len(offer) > 4 and offer[4] is not None else 0
            discount_price = offer[5] if len(offer) > 5 and offer[5] is not None else 0
            
            # Вычисляем скидку безопасно
            if original_price and original_price > 0:
                discount_percent = int((1 - discount_price / original_price) * 100)
            else:
                discount_percent = 0
            
            # Обрезаем длинный текст кнопки
            button_text = f"{offer_title} (-{discount_percent}%)"
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
            builder.button(text=button_text, callback_data=f"offer_{offer_id}")
        except (IndexError, ZeroDivisionError, TypeError) as e:
            # Пропускаем проблемные записи
            import logging
            logging.warning(f"Error creating offer button: {e}, offer={offer}")
            continue

    # Навигация
    nav_row = []
    if start > 0 and store_id is not None:
        prev_off = max(0, start - page_size)
        nav_row.append(("⬅️", f"offers_prev_{store_id}_{prev_off}"))
    if end < total and store_id is not None:
        next_off = end
        nav_row.append(("➡️", f"offers_next_{store_id}_{next_off}"))
    for txt, cb in nav_row:
        builder.button(text=txt, callback_data=cb)

    builder.button(text=get_text(lang, 'back'), callback_data="back_to_stores")
    builder.adjust(1)
    return builder.as_markup()

# ============== ПОИСК И ФИЛЬТРЫ ==============

def filters_keyboard(lang: str = 'ru'):
    """Клавиатура фильтров"""
    builder = InlineKeyboardBuilder()
    builder.button(text="💰 По цене" if lang == 'ru' else "💰 Narx bo'yicha", callback_data="filter_price")
    builder.button(text="🏷 По категории" if lang == 'ru' else "🏷 Kategoriya bo'yicha", callback_data="filter_category")
    builder.button(text="⭐ По рейтингу" if lang == 'ru' else "⭐ Reyting bo'yicha", callback_data="filter_rating")
    builder.button(text="❌ Сбросить" if lang == 'ru' else "❌ Tozalash", callback_data="filter_reset")
    builder.adjust(1)
    return builder.as_markup()

def rating_filter_keyboard():
    """Клавиатура фильтра по рейтингу"""
    builder = InlineKeyboardBuilder()
    builder.button(text="⭐ 4+ звезды", callback_data="rating_4")
    builder.button(text="⭐⭐⭐ 3+ звезды", callback_data="rating_3")
    builder.button(text="⭐⭐ 2+ звезды", callback_data="rating_2")
    builder.button(text="⭐ 1+ звезда", callback_data="rating_1")
    builder.button(text="❌ Без фильтра", callback_data="rating_0")
    builder.adjust(1)
    return builder.as_markup()

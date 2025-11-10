# Локализация для бота Fudly

LANGUAGES = {
    'ru': '🇷🇺 Русский',
    'uz': '🇺🇿 O\'zbekcha'
}

TEXTS = {
    'ru': {
        # Приветствие
        'choose_language': '🌍 Выберите язык / Tilni tanlang',
        'language_changed': '✅ Язык изменён на Русский',
        'welcome': '''🍽 <b>Добро пожаловать в Fudly!</b>

Привет, {name}! 👋

Fudly помогает спасать еду от выбрасывания и экономить ваши деньги!

🛍 Покупайте качественную еду со скидкой до 70%
🏪 Помогайте бизнесу снижать потери
🌍 Заботьтесь об окружающей среде

Для продолжения поделитесь своим номером телефона 📱''',
        
        'welcome_back': '''🍽 <b>С возвращением в Fudly!</b>

Привет, {name}! 👋

📍 Ваш город: {city}

Выберите действие:''',

        'registration_required': '''❌ <b>Требуется регистрация</b>

Для использования бота необходимо поделиться номером телефона.
Нажмите /start для регистрации.''',

        # Кнопки
        'share_phone': 'Поделиться номером',
        'cancel': 'Отмена',
        'available_offers': '🍽 Доступные предложения',
        'my_bookings': '📋 Мои бронирования',
        'stores': '🏪 Все магазины',
        'favorites': '❤️ Избранное',
        'my_city': '🌆 Мой город',
        'profile': '👤 Профиль',
        'become_partner': '🏪 Стать партнером',
        
        # Партнёр - новые короткие названия
        'add_item': '➕ Добавить',
        'my_items': '📦 Мои товары',
        'orders': '🎫 Заказы',
        'today_stats': '📊 Сегодня',
        'back_to_customer': '🔄 Режим покупателя',
        
        # Старые ключи (для совместимости)
        'add_offer': '➕ Добавить',
        'my_offers': '📋 Мои товары',
        'store_bookings': '🎫 Заказы',
        'notifications': '🔔 Уведомления',
        'settings': '⚙️ Настройки',
        
        # Профиль
        'your_profile': '👤 <b>Ваш профиль</b>',
        'name': '📝 Имя',
        'phone': '📱 Телефон',
        'city': '📍 Город',
        'language': '🌍 Язык',
        'role': '👔 Роль',
        'role_seller': 'Партнёр',
        'role_customer': 'Покупатель',
        'switched_to_customer': '🔄 Переключено в режим покупателя',
        'switched_to_seller': '🔄 Переключено в режим партнёра',
        
        # Города
        'your_city': '📍 Ваш город',
        'choose_city': '🌆 <b>Выберите ваш город:</b>',
        'city_changed': '✅ Город изменён на {city}',
        
        # Предложения
        'no_offers': '😔 Пока нет доступных предложений в вашем городе',
        'no_offers_in_store': '😔 В этом магазине пока нет предложений',
        'offers_in_city': '🍽 <b>Доступные предложения в городе {city}</b>\n\nВсего: {count}',
        'offers_found': '🍽 <b>Доступные предложения</b>\n\nНайдено: {count}',
        'all_offers': 'Все предложения',
        'no_active_offers': 'Нет активных предложений',
        'choose_category': '🏪 Выберите категорию заведения:',
        'choose_store': '🏪 Выберите магазин:',
        'choose_offer': '🍽 Выберите предложение:',
        'back': '🔙 Назад',
        'book': '✅ Забронировать',
        'details': 'ℹ️ Подробнее',
        'discount': 'Скидка',
        'available': 'Доступно',
        'time': 'Время',
        'address': 'Адрес',
        'currency': 'сум',
        'unit': 'шт',
        'expires_on': 'Годен до',
        
        # Бронирование
        'booking_success': '''✅ <b>Бронирование успешно!</b>

🏪 {store_name}
🍽 {offer_name}
💰 К оплате: {price} сум

📍 Адрес: {city}, {address}
🕐 Забрать до: {time}

🎫 Код бронирования: <code>{code}</code>

⚠️ Покажите этот код при получении заказа!''',
        
        'my_bookings_empty': '📋 У вас пока нет бронирований.\n\nВыберите предложение из списка! 🍽',
        'active_bookings': '📋 <b>Ваши активные бронирования:</b>\n\nВсего: {count}',
        'cancel_booking': '❌ Отменить бронь',
        'booking_cancelled': '✅ Бронирование отменено',
        'insufficient_stock': '❌ К сожалению, выбранное количество уже недоступно. Обновите список предложений.',
        'error_qty_gt_zero': '❌ Количество должно быть больше 0',
        'error_price_gt_zero': '❌ Цена должна быть больше 0',
        'error_price_too_high': '❌ Слишком большая цена',
        'error_discount_less_than_original': '❌ Цена со скидкой должна быть меньше обычной цены',
        'warn_discount_low': '⚠️ Внимание: скидка меньше 10%. Рекомендуем делать скидку от 30% для привлечения клиентов.',
        'booking_how_many': 'Сколько вы хотите забронировать? (1-{max_qty})',
        
        # Партнёр
        'become_partner_text': '''🏪 <b>Стать партнёром Fudly</b>

Присоединяйтесь к нашей платформе и:

✅ Снижайте потери продукции
✅ Привлекайте новых клиентов
✅ Получайте дополнительный доход
✅ Заботьтесь об экологии

Для регистрации заполните информацию о вашем заведении:

Начнём с выбора города 🌆''',
        
        'store_name': 'Введите название вашего магазина/ресторана:',
        'store_category': 'Выберите категорию заведения:',
        'store_address': 'Введите адрес:',
        'store_description': 'Введите описание (что вы предлагаете):',
        'store_phone': 'Введите контактный телефон:',
        
        'store_registered': '''✅ <b>Заявка отправлена!</b>

🏪 {name}
📍 {city}, {address}
🏷 {category}
📝 {description}
📞 {phone}

⏳ Ожидайте одобрения администратором.
Вы получите уведомление о решении!''',
        
        'store_pending': '''✅ <b>Заявка отправлена на модерацию!</b>

🏪 {name}
📍 {city}, {address}
🏷 {category}
📝 {description}
📞 {phone}

⏳ Ожидайте одобрения администратором.
Обычно это занимает не более 24 часов.''',
        
        'store_approved': '''🎉 <b>Поздравляем!</b>

Ваша заявка на партнёрство <b>ОДОБРЕНА</b>!

Теперь вы можете:
➕ Создавать предложения
📸 Загружать фото товаров
📊 Управлять бронированиями

Желаем успешных продаж!''',
        
        'store_rejected': '''❌ <b>Заявка отклонена</b>

К сожалению, ваша заявка не была одобрена.

Вы можете подать новую заявку с исправленными данными.''',
        
        # Создание предложения
        'choose_store': 'Выберите магазин:',
        'choose_product_category': '🏷 Выберите категорию продукта:',
        'choose_unit': '📏 Выберите единицу измерения:',
        'offer_title': 'Введите название предложения:',
        'offer_description': '📝 Введите описание предложения:',
        'original_price': '💰 Введите обычную цену (в сумах):',
        'discount_price': '💸 Введите цену со скидкой (в сумах):',
        'quantity': '📦 Введите количество порций:',
        'time_from': '🕐 Введите время начала (например: 18:00):',
        'expiry_date': '📅 Введите срок годности (например: 31.12.2025):',
        'time_until': '🕐 Введите время окончания забора (например: 21:00):',
        
        'offer_created': '''✅ <b>Предложение создано!</b>

🍽 {title}
📝 {description}
💰 {original_price} ➜ {discount_price} сум (-{discount}%)
📦 Количество: {quantity}
🕐 {time_from} - {time_until}

Предложение теперь доступно для покупателей!''',
        
        # Массовое создание
        'bulk_create_start': '''📦 <b>Массовое создание предложений</b>

🏪 Магазин: {store_name}

Эта функция позволит создать несколько одинаковых предложений за один раз.
Например: 50 наборов "Завтрак" с одинаковой ценой и временем.

Введите название предложения:''',
        
        'bulk_count': '''🔢 <b>Сколько таких предложений создать?</b>

Введите количество (от 1 до 100):''',
        
        'bulk_created': '''✅ <b>Массовое создание завершено!</b>

📦 Создано предложений: {count}

🍽 {title}
📝 {description}
💰 {original_price} ➜ {discount_price} сум (-{discount}%)
📦 Порций в каждом: {quantity}
📊 Всего порций: {total_quantity}
🕐 {time_from} - {time_until}

Все предложения доступны для покупателей!''',
        
        # Подтверждение выдачи
        'confirm_delivery_prompt': '✅ <b>Подтверждение выдачи заказа</b>\n\nВведите 8-значный код бронирования:',
        'booking_not_found': '❌ Бронирование с таким кодом не найдено',
        'order_confirmed': '''✅ <b>Заказ подтверждён!</b>

Бронирование #{booking_id} завершено
Клиент: {customer_name}
Сумма: {price} сум

Клиент получит уведомление с просьбой оценить ваш магазин.''',
        
        # Рейтинг
        'rate_store': '⭐ <b>Оцените магазин</b>\n\n🏪 {store_name}\n\nКак вам понравилось?',
        'rating_saved': '✅ <b>Спасибо за оценку!</b>\n\nВаш отзыв поможет другим покупателям!',
        'already_rated': 'Вы уже оценили этот заказ',
        
        # Статистика
        'store_stats': '''🏪 <b>{name}</b>
🏷 {category}
📍 {city}, {address}
📝 {description}

⭐ Рейтинг: {rating}/5 ({reviews} отзывов)
📊 Продано: {sales} заказов
💰 Доход: {revenue:,} сум
📦 Активных броней: {pending}''',
        
        # Ошибки
        'error_invalid_number': '❌ Пожалуйста, введите корректное число',
        'error_invalid_time': '❌ Неверный формат времени. Используйте формат ЧЧ:ММ (например: 18:00)',
        'no_stores': '❌ У вас нет одобренных магазинов!',
        'no_approved_stores': '❌ У вас нет одобренных магазинов!\n\n⏳ Дождитесь одобрения вашей заявки администратором.',
        'operation_cancelled': '❌ Операция отменена',
        'no_admin_access': '❌ У вас нет доступа к админ панели',
        'send_photo': '📸 Отправьте фото блюда (или напишите "пропустить")',
        'invalid_range': '❌ От 1 до 100',
        'no_offers_yet': '📊 Пока нет предложений',
        'your_offers': '📊 Ваши предложения ({count}):',
        'no_stores_in_city': '😔 В городе {city} пока нет магазинов',
        'stores_in_city': '🏪 <b>Магазины в городе {city}</b>\n\nВсего: {count}',
        'your_stores': '🏪 Ваши магазины ({count}):',
        'access_denied': '❌ Доступ запрещён',
        'no_pending_stores': '✅ Нет заявок на модерации',
        'pending_stores_count': '⏳ Заявок на модерации: {count}',
        'store_approved_admin': '✅ Магазин одобрен!',
        'store_rejected_admin': '✅ Магазин отклонён!',
        
        # Избранное
        'no_favorites': '😔 У вас пока нет избранных магазинов\n\nДобавьте магазины в избранное, чтобы быстро находить их!',
        'already_in_favorites': '❤️ Уже в избранном!',
        'added_to_favorites': '✅ Добавлено в избранное!',
        'removed_from_favorites': '💔 Удалено из избранного',
        
        # Аналитика
        'not_seller': '❌ Эта функция доступна только партнёрам',
        'no_stores': '😔 У вас пока нет магазинов',
        'select_store_for_analytics': '📊 Выберите магазин для просмотра аналитики:',
        
        # Прочее
        'duplicate': '📋 Дублировать',
        'delete': '❌ Удалить',
        'duplicated': '✅ Предложение продублировано!',
        'deleted': '✅ Предложение удалено',
        'change_language': '🌍 Изменить язык',
        'delete_account': '🗑 Удалить аккаунт',
        
        # Настройки
        'notifications_enabled': '✅ Уведомления включены',
        'notifications_disabled': '🔕 Уведомления отключены',
        'confirm_delete_account': '''⚠️ <b>Удаление аккаунта</b>

Вы уверены что хотите удалить свой аккаунт?

Будут удалены:
• Все ваши данные
• Ваши магазины
• Все предложения
• История бронирований

Это действие необратимо!''',
        'account_deleted': '✅ Ваш аккаунт успешно удалён',
        'yes_delete': '✅ Да, удалить',
        'no_cancel': '❌ Нет, отменить',
        'store_deleted': '✅ Магазин успешно удалён',
        'error_general': '❌ Произошла ошибка. Попробуйте позже.',
        
        # Хардкод-тексты которые нужно было перевести
        'offer_not_found': '❌ Товар не найден',
        'not_your_offer': '❌ Это не ваш товар',
        'edit_unavailable': '📝 Редактирование товара временно недоступно',
        'main_menu': '🏠 Главное меню',
        'time_edit_title': '🕐 Изменение времени забора',
        'time_edit_prompt': 'Введите новое время начала (например: 18:00):',
        'time_end_prompt': 'Введите время окончания (например: 21:00):',
        'time_updated': '✅ Время забора обновлено!',
        'title_saved': '✅ Название сохранено',
        'send_photo_now': '📸 Теперь отправьте фото товара или нажмите кнопку',
        'without_photo': '📝 Без фото',
        'user_not_found': 'Ошибка: пользователь не найден',
        
        # Валидация/лимиты
        'invalid_city': 'Пожалуйста, выберите город из списка.',
        'rate_limit_exceeded': 'Слишком много запросов. Попробуйте позже.',
    },
    
    'uz': {
        # Salomlashish
        'choose_language': '🌍 Выберите язык / Tilni tanlang',
        'language_changed': '✅ Til O\'zbekchaga o\'zgartirildi',
        'welcome': '''🍽 <b>Fudly ga xush kelibsiz!</b>

Salom, {name}! 👋

Fudly oziq-ovqatni isrof bo'lishdan saqlash va pulingizni tejashga yordam beradi!

🛍 Sifatli taomlarni 70% gacha chegirmada sotib oling
🏪 Biznesga yo'qotishlarni kamaytirishda yordam bering
🌍 Atrof-muhitni muhofaza qiling

Davom etish uchun telefon raqamingiz bilan bo'lishing 📱''',
        
        'welcome_back': '''🍽 <b>Fudly ga qaytganingizdan xursandmiz!</b>

Salom, {name}! 👋

📍 Sizning shahringiz: {city}

Harakatni tanlang:''',

        'registration_required': '''❌ <b>Ro'yxatdan o'tish talab qilinadi</b>

Botdan foydalanish uchun telefon raqamingiz bilan bo'lishingiz kerak.
Ro'yxatdan o'tish uchun /start bosing.''',

        # Tugmalar
        'share_phone': 'Raqamni ulashish',
        'cancel': 'Bekor qilish',
        'available_offers': '🍽 Mavjud takliflar',
        'my_bookings': '📋 Mening buyurtmalarim',
        'stores': '🏪 Barcha dokonlar',
        'favorites': '❤️ Sevimlilar',
        'my_city': '🌆 Mening shahrim',
        'profile': '👤 Profil',
        'become_partner': '🏪 Hamkor bolish',
        
        # Hamkor - yangi qisqa nomlar
        'add_item': '➕ Qo\'shish',
        'my_items': '📦 Mening mahsulotlarim',
        'orders': '🎫 Buyurtmalar',
        'today_stats': '📊 Bugun',
        'back_to_customer': '� Xaridor rejimi',
        
        # Eski kalitlar (muvofiqligi uchun)
        'add_offer': '➕ Qo\'shish',
        'my_offers': '📦 Mening mahsulotlarim',
        'store_bookings': '🎫 Buyurtmalar',
        'notifications': '🔔 Bildirishnomalar',
        'settings': '⚙️ Sozlamalar',
        
        # Profil
         'choose_unit': '📏 O‘lchov birliklarini tanlang:',
         'choose_product_category': '🏷 Mahsulot kategoriyasini tanlang:',
        'your_profile': '👤 <b>Sizning profilingiz</b>',
        'name': '📝 Ism',
        'phone': '📱 Telefon',
        'city': '📍 Shahar',
        'language': '🌍 Til',
        'role': '👔 Rol',
        'role_seller': 'Hamkor',
        'role_customer': 'Xaridor',
        'switched_to_customer': '🔄 Xaridor rejimiga ogirildi',
        'switched_to_seller': '🔄 Hamkor rejimiga ogirildi',
        
        # Shaharlar
        'your_city': '📍 Sizning shahringiz',
        'choose_city': '🌆 <b>Shahringizni tanlang:</b>',
        'city_changed': '✅ Shahar {city}ga o\'zgartirildi',
        
        # Takliflar
        'no_offers': '😔 Hozircha sizning shahringizda takliflar yo\'q',
        'no_offers_in_store': '😔 Bu dokonda hali takliflar yo\'q',
        'offers_in_city': '🍽 <b>{city} shahridagi mavjud takliflar</b>\n\nJami: {count}',
        'offers_found': '🍽 <b>Mavjud takliflar</b>\n\nTopildi: {count}',
        'all_offers': 'Barcha takliflar',
        'no_active_offers': 'Faol takliflar yo\'q',
        'choose_category': '🏪 Kategoriyani tanlang:',
        'choose_store': '🏪 Dokonni tanlang:',
        'choose_offer': '🍽 Taklifni tanlang:',
        'back': '🔙 Orqaga',
        'book': '✅ Buyurtma qilish',
        'details': 'ℹ️ Batafsil',
        'discount': 'Chegirma',
        'available': 'Mavjud',
        'time': 'Vaqt',
        'address': 'Manzil',
        'currency': 'so\'m',
        'unit': 'dona',
        'expires_on': 'Yaroqlilik muddati',
        
        # Buyurtma
        'booking_success': '''✅ <b>Buyurtma muvaffaqiyatli!</b>

🏪 {store_name}
🍽 {offer_name}
💰 To'lash kerak: {price} so'm

📍 Manzil: {city}, {address}
🕐 Olish vaqti: {time}

🎫 Buyurtma kodi: <code>{code}</code>

⚠️ Buyurtmani olishda bu kodni ko'rsating!''',
        
        'my_bookings_empty': '📋 Sizda hali buyurtmalar yo\'q.\n\nTakliflar ro\'yxatidan tanlang! 🍽',
        'active_bookings': '📋 <b>Sizning faol buyurtmalaringiz:</b>\n\nJami: {count}',
        'cancel_booking': '❌ Buyurtmani bekor qilish',
        'booking_cancelled': '✅ Buyurtma bekor qilindi',
        'insufficient_stock': '❌ Afsuski, tanlangan miqdor endi mavjud emas. Takliflar ro‘yxatini yangilang.',
        'error_qty_gt_zero': '❌ Miqdor 0 dan katta bo‘lishi kerak',
        'error_price_gt_zero': '❌ Narx 0 dan katta bo‘lishi kerak',
        'error_price_too_high': '❌ Juda katta narx',
        'error_discount_less_than_original': '❌ Chegirma narxi oddiy narxdan kichik bo‘lishi kerak',
        'warn_discount_low': '⚠️ Diqqat: chegirma 10% dan kichik. Mijozlarni jalb qilish uchun 30% va undan yuqori tavsiya etamiz.',
        'booking_how_many': 'Nechta buyurtma qilmoqchisiz? (1-{max_qty})',
        
        # Hamkor
        'become_partner_text': '''🏪 <b>Fudly hamkori bo\'ling</b>

Platformamizga qo\'shiling va:

✅ Mahsulot yo'qotishlarini kamaytiring
✅ Yangi mijozlarni jalb qiling
✅ Qo'shimcha daromad oling
✅ Ekologiyaga g'amxo'rlik qiling

Ro'yxatdan o'tish uchun muassasangiz haqida ma'lumot to'ldiring:

Shaharni tanlashdan boshlaymiz 🌆''',
        
        'store_name': 'Do\'kon/Restoran nomini kiriting:',
        'store_category': 'Muassasa kategoriyasini tanlang:',
        'store_address': 'Manzilni kiriting:',
        'store_description': 'Ta\'rifni kiriting (nima taklif qilasiz):',
        'store_phone': 'Aloqa telefonini kiriting:',
        
        'store_registered': '''✅ <b>Ariza yuborildi!</b>

🏪 {name}
📍 {city}, {address}
🏷 {category}
📝 {description}
📞 {phone}

⏳ Administrator tomonidan tasdiqlanishini kuting.
Qaror haqida xabar olasiz!''',
        
        'store_pending': '''✅ <b>Ariza moderatsiyaga yuborildi!</b>

🏪 {name}
📍 {city}, {address}
🏷 {category}
📝 {description}
📞 {phone}

⏳ Administrator tomonidan tasdiqlanishini kuting.
Odatda bu 24 soatdan ortiq vaqt olmaydi.''',
        
        'store_approved': '''🎉 <b>Tabriklaymiz!</b>

Hamkorlik uchun arizangiz <b>TASDIQLANDI</b>!

Endi siz qila olasiz:
➕ Takliflar yaratish
📸 Mahsulot fotosuratlarini yuklash
📊 Buyurtmalarni boshqarish

Muvaffaqiyatli savdo tilaymiz!''',
        
        'store_rejected': '''❌ <b>Ariza rad etildi</b>

Afsuski, arizangiz tasdiqlanmadi.

Tuzatilgan ma'lumotlar bilan yangi ariza topshirishingiz mumkin.''',
        
        # Takлиф yaratиш
        'choose_store': 'Do\'konni tanlang:',
        'offer_title': 'Taklif nomini kiriting:',
        'offer_description': '📝 Taklif ta\'rifini kiriting:',
        'original_price': '💰 Oddiy narxni kiriting (so\'mda):',
        'discount_price': '💸 Chegirmali narxni kiriting (so\'mda):',
        'quantity': '📦 Portsialar sonini kiriting:',
        'time_from': '🕐 Boshlanish vaqtini kiriting (masalan: 18:00):',
        'expiry_date': '📅 Yaroqlilik muddatini kiriting (masalan: 31.12.2025):',
        'time_until': '🕐 Olib ketish tugash vaqtini kiriting (masalan: 21:00):',
        
        'offer_created': '''✅ <b>Taklif yaratildi!</b>

🍽 {title}
📝 {description}
💰 {original_price} ➜ {discount_price} so\'m (-{discount}%)
📦 Soni: {quantity}
🕐 {time_from} - {time_until}

Taklif endi xaridorlar uchun mavjud!''',
        
        # Ommaviy yaratish
        'bulk_create_start': '''📦 <b>Ommaviy taklif yaratish</b>

🏪 Do\'kon: {store_name}

Bu funksiya bir nechta bir xil takliflarni bir vaqtning o'zida yaratishga imkon beradi.
Masalan: 50 ta "Nonushta" to'plami bir xil narx va vaqt bilan.

Taklif nomini kiriting:''',
        
        'bulk_count': '''🔢 <b>Nechta bunday taklif yaratilsin?</b>

Sonini kiriting (1 dan 100 gacha):''',
        
        'bulk_created': '''✅ <b>Ommaviy yaratish tugadi!</b>

📦 Yaratilgan takliflar: {count}

🍽 {title}
📝 {description}
💰 {original_price} ➜ {discount_price} so\'m (-{discount}%)
📦 Har birida: {quantity}
📊 Jami portsialar: {total_quantity}
🕐 {time_from} - {time_until}

Barcha takliflar xaridorlar uchun mavjud!''',
        
        # Berishni tasdiqlash
        'confirm_delivery_prompt': '✅ <b>Buyurtma berishni tasdiqlash</b>\n\n8 xonali buyurtma kodini kiriting:',
        'booking_not_found': '❌ Bunday kodli buyurtma topilmadi',
        'order_confirmed': '''✅ <b>Buyurtma tasdiqlandi!</b>

Buyurtma #{booking_id} tugallandi
Mijoz: {customer_name}
Summa: {price} so\'m

Mijoz do'koningizni baholash uchun xabar oladi.''',
        
        # Baho
        'rate_store': '⭐ <b>Do\'konni baholang</b>\n\n🏪 {store_name}\n\nSizga qanday yoqdi?',
        'rating_saved': '✅ <b>Baholaganingiz uchun rahmat!</b>\n\nSizning fikringiz boshqa xaridorlarga yordam beradi!',
        'already_rated': 'Siz bu buyurtmani allaqachon baholagansiz',
        
        # Statistika
        'store_stats': '''🏪 <b>{name}</b>
🏷 {category}
📍 {city}, {address}
📝 {description}

⭐ Reyting: {rating}/5 ({reviews} ta sharh)
📊 Sotilgan: {sales} ta buyurtma
💰 Daromad: {revenue:,} so\'m
📦 Faol buyurtmalar: {pending}''',
        
        # Xatolar
        'error_invalid_number': '❌ Iltimos, to\'g\'ri raqam kiriting',
        'error_invalid_time': '❌ Noto\'g\'ri vaqt formati. HH:MM formatidan foydalaning (masalan: 18:00)',
        'no_stores': '❌ Sizda tasdiqlangan dokonlar yoq!',
        'no_approved_stores': '❌ Sizda tasdiqlangan dokonlar yoq!\n\n⏳ Administrator tomonidan arizangizni tasdiqlanishini kuting.',
        'operation_cancelled': '❌ Operatsiya bekor qilindi',
        'no_admin_access': '❌ Sizda admin paneliga kirish huquqi yoq',
        'send_photo': '📸 Taom rasmini yuboring (yoki "otkazib yuborish" deb yozing)',
        'invalid_range': '❌ 1 dan 100 gacha',
        'no_offers_yet': '📊 Hali takliflar yoq',
        'your_offers': '📊 Sizning takliflaringiz ({count}):',
        'no_stores_in_city': '😔 {city} shahrida hali dokonlar yoq',
        'stores_in_city': '🏪 <b>{city} shahridagi dokonlar</b>\n\nJami: {count}',
        'your_stores': '🏪 Sizning dokonlaringiz ({count}):',
        'access_denied': '❌ Kirish taqiqlangan',
        'no_pending_stores': '✅ Moderatsiyada arizalar yoq',
        'pending_stores_count': '⏳ Moderatsiyadagi arizalar: {count}',
        'store_approved_admin': '✅ Dokon tasdiqlandi!',
        'store_rejected_admin': '✅ Dokon rad etildi!',
        
        # Sevimlilar
        'no_favorites': '😔 Sizda hali sevimli dokonlar yoq\n\nDokonlarni sevimlilarga qo\'shing, tez topish uchun!',
        'already_in_favorites': '❤️ Allaqachon sevimlilarda!',
        'added_to_favorites': '✅ Sevimlilarga qo\'shildi!',
        'removed_from_favorites': '💔 Sevimlilardan o\'chirildi',
        
        # Analitika
        'not_seller': '❌ Bu funksiya faqat hamkorlar uchun',
        'no_stores': '😔 Sizda hali dokonlar yoq',
        'select_store_for_analytics': '📊 Analitika uchun dokonni tanlang:',
        
        # Boshqa
        'duplicate': '📋 Nusxalash',
        'delete': '❌ O\'chirish',
        'duplicated': '✅ Taklif nusxalandi!',
        'deleted': '✅ Taklif o\'chirildi',
        'change_language': '🌍 Tilni o\'zgartirish',
        'delete_account': '🗑 Akkauntni o\'chirish',
        
        # Sozlamalar
        'notifications_enabled': '✅ Bildirishnomalar yoqildi',
        'notifications_disabled': '🔕 Bildirishnomalar o\'chirildi',
        'confirm_delete_account': '''⚠️ <b>Akkauntni o\'chirish</b>

Akkauntingizni o\'chirishni xohlaysizmi?

O\'chiriladi:
• Barcha ma\'lumotlaringiz
• Dokonlaringiz
• Barcha takliflar
• Buyurtmalar tarixi

Bu harakatni qaytarib bo\'lmaydi!''',
        'account_deleted': '✅ Akkauntingiz muvaffaqiyatli o\'chirildi',
        'yes_delete': '✅ Ha, o\'chirish',
        'no_cancel': '❌ Yo\'q, bekor qilish',
        'store_deleted': '✅ Dokon muvaffaqiyatli o\'chirildi',
        'error_general': '❌ Xatolik yuz berdi. Keyinroq urinib ko\'ring.',
        
        # Qo'shimcha tarjimalar (xardkod-tekstlar)
        'offer_not_found': '❌ Mahsulot topilmadi',
        'not_your_offer': '❌ Bu sizning mahsulotingiz emas',
        'edit_unavailable': '📝 Mahsulotni tahrirlash vaqtincha mavjud emas',
        'main_menu': '🏠 Asosiy menyu',
        'time_edit_title': '🕐 Olib ketish vaqtini o\'zgartirish',
        'time_edit_prompt': 'Yangi boshlanish vaqtini kiriting (masalan: 18:00):',
        'time_end_prompt': 'Tugash vaqtini kiriting (masalan: 21:00):',
        'time_updated': '✅ Olib ketish vaqti yangilandi!',
        'title_saved': '✅ Nom saqlandi',
        'send_photo_now': '📸 Endi mahsulot rasmini yuboring yoki tugmani bosing',
        'without_photo': '📝 Fotosiz',
        'user_not_found': 'Xato: foydalanuvchi topilmadi',
        
        # Validatsiya/limitlar
        'invalid_city': 'Iltimos, ro\'yxatdan shaharni tanlang.',
        'rate_limit_exceeded': 'Juda ko\'p so\'rovlar. Keyinroq urinib ko\'ring.',
    }
}

def get_text(lang: str, key: str, **kwargs) -> str:
    """Получить текст на нужном языке с форматированием
    
    Args:
        lang: Код языка ('ru' или 'uz')
        key: Ключ текста из TEXTS
        **kwargs: Параметры для форматирования строки
        
    Returns:
        Отформатированная строка текста или сам ключ, если текст не найден
    """
    try:
        texts = TEXTS.get(lang, TEXTS.get('ru', {}))
        text = texts.get(key, key)
        
        # Если текст не найден, пробуем русский
        if text == key and lang != 'ru':
            text = TEXTS.get('ru', {}).get(key, key)
        
        # Форматируем, если есть параметры и текст содержит плейсхолдеры
        if kwargs and text != key:
            try:
                return text.format(**kwargs)
            except (KeyError, ValueError) as e:
                # Если форматирование не удалось, возвращаем текст без форматирования
                import logging
                logging.warning(f"Format error in get_text: {e}, key={key}, lang={lang}")
                return text
        
        return text
    except Exception as e:
        import logging
        logging.error(f"Error in get_text: {e}, key={key}, lang={lang}")
        return key

def get_language_name(lang: str) -> str:
    """Получить название языка"""
    return LANGUAGES.get(lang, LANGUAGES['ru'])

def get_cities(lang: str) -> list:
    """Получить список городов на нужном языке"""
    return [
        "Ташкент" if lang == 'ru' else "Toshkent",
        "Самарканд" if lang == 'ru' else "Samarqand",
        "Бухара" if lang == 'ru' else "Buxoro",
        "Андижан" if lang == 'ru' else "Andijon",
        "Наманган" if lang == 'ru' else "Namangan",
        "Фергана" if lang == 'ru' else "Farg'ona",
        "Хива" if lang == 'ru' else "Xiva",
        "Нукус" if lang == 'ru' else "Nukus"
    ]

def get_categories(lang: str) -> list:
    """Получить список категорий на нужном языке"""
    if lang == 'ru':
        return ["Ресторан", "Кафе", "Пекарня", "Супермаркет", "Кондитерская", "Фастфуд"]
    else:
        return ["Restoran", "Kafe", "Nonvoyxona", "Supermarket", "Qandolatxona", "Fastfud"]

def normalize_category(category: str) -> str:
    """Нормализовать категорию к русскому для БД"""
    mapping = {
        'Restoran': 'Ресторан',
        'Kafe': 'Кафе',
        'Nonvoyxona': 'Пекарня',
        'Supermarket': 'Супермаркет',
        'Qandolatxona': 'Кондитерская',
        'Fastfud': 'Фастфуд'
    }
    return mapping.get(category, category)

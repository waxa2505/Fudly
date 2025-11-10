# type: ignore
import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

# Простое логирование (fallback если нет logging_config)
try:
    from logging_config import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# Импорт кэша
try:
    from cache import cache
except ImportError:
    # Простой кэш-заглушка (для совместимости)
    class SimpleCache:
        """Простая заглушка кэша без зависимостей"""
        def get(self, key):
            return None
        
        def set(self, key, value, ex=None):
            pass
        
        def delete(self, key):
            pass
    
    cache = SimpleCache()

# Module-level settings
DB_PATH = os.environ.get('DATABASE_PATH', 'fudly.db')

class Database:
    def __init__(self, db_name: str = "fudly.db"):
        self.db_name = db_name or DB_PATH
        self.init_db()
    
    def get_connection(self):
        """Возвращает подключение к базе данных"""
        conn = sqlite3.connect(self.db_name, timeout=int(os.environ.get('DB_TIMEOUT', 30)))
        try:
            conn.execute('PRAGMA journal_mode=WAL')
        except Exception:
            pass
        return conn
    
    def init_db(self):
        """Инициализация базы данных"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                phone TEXT,
                city TEXT DEFAULT 'Ташкент',
                language TEXT DEFAULT 'ru',
                role TEXT DEFAULT 'customer',
                is_admin INTEGER DEFAULT 0,
                notifications_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица ресторанов/магазинов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stores (
                store_id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                name TEXT NOT NULL,
                city TEXT NOT NULL,
                address TEXT,
                description TEXT,
                category TEXT DEFAULT 'Ресторан',
                phone TEXT,
                status TEXT DEFAULT 'pending',
                rejection_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (owner_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица предложений еды
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS offers (
                offer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER,
                title TEXT NOT NULL,
                description TEXT,
                original_price REAL,
                discount_price REAL,
                quantity INTEGER DEFAULT 1,
                available_from TEXT,
                available_until TEXT,
                expiry_date TEXT,
                status TEXT DEFAULT 'active',
                photo TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (store_id) REFERENCES stores(store_id)
            )
        ''')
        
        # Таблица бронирований
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bookings (
                booking_id INTEGER PRIMARY KEY AUTOINCREMENT,
                offer_id INTEGER,
                user_id INTEGER,
                status TEXT DEFAULT 'pending',
                booking_code TEXT,
                pickup_time TEXT,
                quantity INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (offer_id) REFERENCES offers(offer_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Добавляем поле quantity если его нет (для старых БД)
        try:
            cursor.execute('ALTER TABLE bookings ADD COLUMN quantity INTEGER DEFAULT 1')
            conn.commit()
        except:
            pass  # Поле уже существует
        
        # Добавляем поле expiry_date если его нет (для старых БД)
        try:
            cursor.execute('ALTER TABLE offers ADD COLUMN expiry_date TEXT')
            conn.commit()
        except Exception:
            pass  # Поле уже существует
        
        # Добавляем поле unit если его нет (для старых БД)
        try:
            cursor.execute("ALTER TABLE offers ADD COLUMN unit TEXT DEFAULT 'шт'")
            conn.commit()
        except Exception:
            pass  # Поле уже существует
        
        # Добавляем поле category если его нет (для старых БД)
        try:
            cursor.execute("ALTER TABLE offers ADD COLUMN category TEXT DEFAULT 'other'")
            conn.commit()
        except Exception:
            pass  # Поле уже существует
        
        # ==================== СОЗДАНИЕ ИНДЕКСОВ ДЛЯ ОПТИМИЗАЦИИ ====================
        # Индексы для быстрого поиска по часто используемым полям
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_status ON offers(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_expiry ON offers(expiry_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_store ON offers(store_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_category ON offers(category)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_offer ON bookings(offer_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stores_owner ON stores(owner_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stores_status ON stores(status)')
            conn.commit()
        except Exception as e:
            pass  # Индексы уже существуют
        
        # Таблица уведомлений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                is_read INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица рейтингов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ratings (
                rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id INTEGER,
                user_id INTEGER,
                store_id INTEGER,
                rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (booking_id) REFERENCES bookings(booking_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (store_id) REFERENCES stores(store_id)
            )
        ''')
        
        # Таблица избранных магазинов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS favorites (
                favorite_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                store_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (store_id) REFERENCES stores(store_id),
                UNIQUE(user_id, store_id)
            )
        ''')
        
        # Таблица промокодов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                promo_id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                discount_percent INTEGER DEFAULT 0,
                discount_amount REAL DEFAULT 0,
                max_uses INTEGER DEFAULT 1,
                current_uses INTEGER DEFAULT 0,
                valid_until TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица использования промокодов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS promo_usage (
                usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                promo_id INTEGER,
                booking_id INTEGER,
                discount_applied REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (promo_id) REFERENCES promocodes(promo_id),
                FOREIGN KEY (booking_id) REFERENCES bookings(booking_id)
            )
        ''')
        
        # Таблица рефералов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                referral_id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                bonus_given INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referred_id) REFERENCES users(user_id)
            )
        ''')
        
        # Добавляем поле bonus_balance в users если его нет
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN bonus_balance REAL DEFAULT 0')
            conn.commit()
        except:
            pass
        
        # Добавляем поле referral_code в users если его нет
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN referral_code TEXT UNIQUE')
            conn.commit()
        except:
            pass
        
        try:
            conn.close()
        except Exception:
            pass

        # create supporting indexes (best-effort)
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stores_city_status ON stores(city, status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_store_status ON offers(store_id, status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_created ON offers(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_expiry ON offers(expiry_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_quantity ON offers(quantity)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_status_quantity_expiry ON offers(status, quantity, expiry_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_offer ON bookings(offer_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_store ON ratings(store_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_user ON ratings(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_favorites_user_store ON favorites(user_id, store_id)')
            conn.commit()
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    # Методы для пользователей
    def add_user(self, user_id: int, username: Optional[str] = None, first_name: Optional[str] = None, role: str = 'customer', city: str = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO users (user_id, username, first_name, role, city)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, username, first_name, role, city))
        conn.commit()
        try:
            conn.close()
        except Exception:
            pass
        try:
            cache.delete('offers:all')
        except Exception:
            pass
    
    def get_user(self, user_id: int) -> Optional[Tuple]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            user = cursor.fetchone()
            return user
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def update_user_city(self, user_id: int, city: str):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET city = ? WHERE user_id = ?', (city, user_id))
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def update_user_role(self, user_id: int, role: str):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET role = ? WHERE user_id = ?', (role, user_id))
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def update_user_phone(self, user_id: int, phone: str):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET phone = ? WHERE user_id = ?', (phone, user_id))
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def update_user_language(self, user_id: int, language: str):
        """Обновить язык пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET language = ? WHERE user_id = ?', (language, user_id))
        conn.commit()
        conn.close()
    
    def get_user_language(self, user_id: int) -> str:
        """Получить язык пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT language FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 'ru'
    
    # Методы для магазинов
    def add_store(self, owner_id: int, name: str, city: str, address: Optional[str] = None, description: Optional[str] = None, category: str = 'Ресторан', phone: Optional[str] = None) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO stores (owner_id, name, city, address, description, category, phone, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        ''', (owner_id, name, city, address, description, category, phone))
        store_id = cursor.lastrowid
        conn.commit()
        try:
            conn.close()
        except Exception:
            pass
        # invalidate relevant cache keys
        try:
            cache.delete(f'stores:city:{city}')
            cache.delete('offers:all')
        except Exception:
            pass
        return store_id
    
    def get_user_stores(self, owner_id: int) -> List[Tuple]:
        """Получить ВСЕ магазины пользователя (любой статус)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT s.*, u.first_name, u.username 
            FROM stores s
            LEFT JOIN users u ON s.owner_id = u.user_id
            WHERE s.owner_id = ?
            ORDER BY s.created_at DESC
        ''', (owner_id,))
        stores = cursor.fetchall()
        conn.close()
        return stores
    
    def get_approved_stores(self, owner_id: int) -> List[Tuple]:
        """Получить только ОДОБРЕННЫЕ магазина пользователя"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM stores WHERE owner_id = ? AND status = "active"', (owner_id,))
            stores = cursor.fetchall()
            return stores
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def get_store(self, store_id: int) -> Optional[Tuple]:
        key = f'store:{store_id}'
        try:
            cached = cache.get(key)
            if cached is not None:
                return cached
        except Exception:
            pass

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM stores WHERE store_id = ?', (store_id,))
        store = cursor.fetchone()
        try:
            conn.close()
        except Exception:
            pass

        try:
            cache.set(key, store, ex=int(os.environ.get('CACHE_TTL_SECONDS', 300)))
        except Exception:
            pass

        return store
    
    def get_stores_by_city(self, city: str) -> List[Tuple]:
        key = f'stores:city:{city}'
        try:
            cached = cache.get(key)
            if cached is not None:
                return cached
        except Exception:
            pass

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM stores WHERE city = ? AND status = "active"', (city,))
        stores = cursor.fetchall()
        try:
            conn.close()
        except Exception:
            pass

        try:
            cache.set(key, stores, ex=int(os.environ.get('CACHE_TTL_SECONDS', 300)))
        except Exception:
            pass

        return stores
    
    def _format_datetime_field(self, time_input: str) -> str:
        """
        Преобразует различные форматы времени в стандартный формат YYYY-MM-DD HH:MM
        """
        from datetime import datetime, timedelta
        
        if not time_input:
            return ""
            
        time_input = time_input.strip()
        
        # Если уже в правильном формате - возвращаем как есть
        try:
            datetime.strptime(time_input, '%Y-%m-%d %H:%M')
            return time_input
        except ValueError:
            pass
        
        current_date = datetime.now()
        
        # Обрабатываем формат HH:MM (например "21:00")
        try:
            time_obj = datetime.strptime(time_input, '%H:%M')
            # Добавляем сегодняшнюю дату
            result_dt = current_date.replace(
                hour=time_obj.hour, 
                minute=time_obj.minute, 
                second=0, 
                microsecond=0
            )
            # Если время уже прошло сегодня, переносим на завтра
            if result_dt <= current_date:
                result_dt += timedelta(days=1)
            return result_dt.strftime('%Y-%m-%d %H:%M')
        except ValueError:
            pass
        
        # Обрабатываем формат HH (например "21")
        try:
            hour = int(time_input)
            if 0 <= hour <= 23:
                result_dt = current_date.replace(
                    hour=hour, 
                    minute=0, 
                    second=0, 
                    microsecond=0
                )
                # Если время уже прошло сегодня, переносим на завтра
                if result_dt <= current_date:
                    result_dt += timedelta(days=1)
                return result_dt.strftime('%Y-%m-%d %H:%M')
        except ValueError:
            pass
        
        # Если ничего не подходит, возвращаем исходное значение
        return time_input

    # Методы для предложений
    def add_offer(self, store_id: int, title: str, description: str, original_price: float, 
                  discount_price: float, quantity: int, available_from: str, available_until: str, 
                  photo: str = None, expiry_date: str = None, unit: str = 'шт', category: str = 'other') -> int:
        
        # Приводим время к стандартному формату
        formatted_from = self._format_datetime_field(available_from)
        formatted_until = self._format_datetime_field(available_until)
        
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO offers (store_id, title, description, original_price, discount_price, 
                              quantity, available_from, available_until, expiry_date, status, photo, unit, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
        ''', (store_id, title, description, original_price, discount_price, quantity, formatted_from, formatted_until, expiry_date, photo, unit, category))
        offer_id = cursor.lastrowid
        conn.commit()
        try:
            conn.close()
        except Exception:
            pass
        # invalidate related caches
        try:
            cache.delete('offers:all')
            cache.delete(f'offers:store:{store_id}')
            cache.delete(f'store:{store_id}')
        except Exception:
            pass
        return offer_id
    
    def get_active_offers(self, city: str = None, store_id: int = None) -> List[Tuple]:
        # Cache keys: offers:all, offers:city:<city>, offers:store:<id>
        cache_key = 'offers:all'
        if store_id:
            cache_key = f'offers:store:{store_id}'
        elif city:
            cache_key = f'offers:city:{city}'
        try:
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass

        conn = self.get_connection()
        cursor = conn.cursor()
        
        if store_id:
            # Фильтр по конкретному магазину
            cursor.execute('''
                SELECT o.*, s.name as store_name, s.address, s.city, s.category
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE o.status = 'active' AND o.quantity > 0 AND s.store_id = ? AND s.status = 'active'
                    AND date(o.expiry_date) >= date('now')
                ORDER BY o.created_at DESC
            ''', (store_id,))
        elif city:
            # Фильтр по городу
            cursor.execute('''
                SELECT o.*, s.name as store_name, s.address, s.city, s.category
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE o.status = 'active' AND o.quantity > 0 AND s.city = ? AND s.status = 'active'
                    AND date(o.expiry_date) >= date('now')
                ORDER BY o.created_at DESC
            ''', (city,))
        else:
            # Все предложения
            cursor.execute('''
                SELECT o.*, s.name as store_name, s.address, s.city, s.category
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE o.status = 'active' AND o.quantity > 0 AND s.status = 'active'
                    AND date(o.expiry_date) >= date('now')
                ORDER BY o.created_at DESC
            ''')
        
        offers = cursor.fetchall()
        try:
            conn.close()
        except Exception:
            pass
        
        # Фильтруем товары с истёкшим сроком годности
        from datetime import datetime
        valid_offers = []
        for offer in offers:
            # Проверяем срок годности если он указан (индекс 12 - expiry_date после ALTER TABLE)
            if len(offer) > 12 and offer[12]:
                try:
                    # Преобразуем дату из формата DD.MM.YYYY или YYYY-MM-DD
                    expiry_str = str(offer[12])
                    if '.' in expiry_str:
                        expiry_parts = expiry_str.split('.')
                        if len(expiry_parts) == 3:
                            expiry_date = datetime(int(expiry_parts[2]), int(expiry_parts[1]), int(expiry_parts[0]))
                        else:
                            valid_offers.append(offer)
                            continue
                    elif '-' in expiry_str:
                        expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                    else:
                        valid_offers.append(offer)
                        continue
                    # Если срок годности не истёк
                    if expiry_date >= datetime.now():
                        valid_offers.append(offer)
                except:
                    valid_offers.append(offer)  # Ошибка парсинга - показываем
            else:
                valid_offers.append(offer)  # Нет срока годности - показываем
        
        # cache result
        try:
            cache.set(cache_key, valid_offers, ex=int(os.environ.get('CACHE_TTL_SECONDS', 120)))
        except Exception:
            pass

        return valid_offers
    
    def get_offer(self, offer_id: int) -> Optional[Tuple]:
        """Получить предложение с информацией о магазине.
        
        Returns tuple structure (АКТУАЛЬНАЯ структура после ALTER TABLE):
        [0] offer_id
        [1] store_id
        [2] title
        [3] description
        [4] original_price
        [5] discount_price
        [6] quantity
        [7] available_from
        [8] available_until
        [9] status
        [10] photo
        [11] created_at
        [12] expiry_date
        [13] unit
        [14] category
        После JOIN добавляются:
        [15] store_name (from stores)
        [16] address (from stores)
        [17] city (from stores)
        [18] category (from stores - category магазина)
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT o.*, s.name as store_name, s.address, s.city, s.category
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE o.offer_id = ?
            ''', (offer_id,))
            offer = cursor.fetchone()
            return offer
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def get_store_offers(self, store_id: int) -> List[Tuple]:
        """Получить товары магазина с информацией о магазине (оптимизировано с JOIN)"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT o.*, s.name as store_name, s.address, s.city, s.category as store_category
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE o.store_id = ? AND o.status != "deleted"
                ORDER BY o.created_at DESC
            ''', (store_id,))
            offers = cursor.fetchall()
            return offers
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def update_offer_quantity(self, offer_id: int, new_quantity: int):
        """Обновить количество товара и автоматически управлять статусом"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if new_quantity <= 0:
            # Если товар закончился - ставим quantity=0 и деактивируем
            cursor.execute('UPDATE offers SET quantity = 0, status = ? WHERE offer_id = ?', ('inactive', offer_id))
        else:
            # Если товар есть - активируем (на случай возврата при отмене)
            cursor.execute('UPDATE offers SET quantity = ?, status = ? WHERE offer_id = ?', (new_quantity, 'active', offer_id))
        
        conn.commit()
        conn.close()
    
    def increment_offer_quantity(self, offer_id: int, amount: int = 1):
        """Увеличить количество товара (при отмене бронирования)"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            # Получаем текущее количество
            cursor.execute('SELECT quantity FROM offers WHERE offer_id = ?', (offer_id,))
            row = cursor.fetchone()
            if row:
                current_qty = row[0] if row[0] is not None else 0
                new_qty = current_qty + amount
                self.update_offer_quantity(offer_id, new_qty)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def deactivate_offer(self, offer_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE offers SET status = ? WHERE offer_id = ?', ('inactive', offer_id))
        conn.commit()
        try:
            conn.close()
        except Exception:
            pass
        try:
            cache.delete('offers:all')
        except Exception:
            pass
    
    def activate_offer(self, offer_id: int):
        """Активировать товар"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE offers SET status = ? WHERE offer_id = ?', ('active', offer_id))
        conn.commit()
        try:
            conn.close()
        except Exception:
            pass
        try:
            cache.delete('offers:all')
        except Exception:
            pass
    
    def update_offer_expiry(self, offer_id: int, new_expiry: str):
        """Обновить срок годности товара"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE offers SET expiry_date = ? WHERE offer_id = ?', (new_expiry, offer_id))
        conn.commit()
        try:
            conn.close()
        except Exception:
            pass
        try:
            cache.delete('offers:all')
        except Exception:
            pass
    
    def delete_offer(self, offer_id: int) -> bool:
        """Удалить товар (установить статус deleted)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE offers SET status = "deleted" WHERE offer_id = ?', (offer_id,))
        conn.commit()
        try:
            conn.close()
        except Exception:
            pass
        try:
            cache.delete('offers:all')
        except Exception:
            pass
        return True
    
    def delete_expired_offers(self):
        """Удаляет предложения с истёкшим сроком годности"""
        from datetime import datetime
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Деактивируем товары с истёкшим сроком годности
        cursor.execute('''
            UPDATE offers 
            SET status = 'inactive' 
            WHERE status = 'active' 
            AND expiry_date IS NOT NULL
            AND date(expiry_date) < date('now')
        ''')
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        try:
            conn.close()
        except Exception:
            pass
            
        # Invalidate offers cache when expiry cleanups happen
        try:
            cache.delete('offers:all')
        except Exception:
            pass
            
        return deleted_count
    
    # Методы для бронирований
    def create_booking(self, offer_id: int, user_id: int, booking_code: str, quantity: int = 1) -> int:
        """Создать бронирование (не атомарное - используйте create_booking_atomic для предотвращения race conditions)"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO bookings (offer_id, user_id, booking_code, status, quantity)
                VALUES (?, ?, ?, 'pending', ?)
            ''', (offer_id, user_id, booking_code, quantity))
            booking_id = cursor.lastrowid
            conn.commit()
            return booking_id
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def create_booking_atomic(self, offer_id: int, user_id: int, quantity: int = 1) -> Tuple[bool, Optional[int], Optional[str]]:
        """Атомарно резервирует товар и создает бронирование внутри одной транзакции.
        
        Args:
            offer_id (int): ID предложения для бронирования
            user_id (int): ID пользователя
            quantity (int, optional): Количество единиц товара. По умолчанию 1.
            
        Returns:
            Tuple[bool, Optional[int], Optional[str]]: 
            - ok: True если бронирование создано успешно
            - booking_id: ID созданного бронирования или None при ошибке
            - booking_code: Код бронирования или None при ошибке
            
        Note:
            Использует транзакцию SQLite с IMMEDIATE для предотвращения race conditions.
            Уменьшает quantity предложения атомарно перед созданием бронирования.
        """
        import random
        import string
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Начинаем транзакцию IMMEDIATE для предотвращения race conditions
            cursor.execute('BEGIN IMMEDIATE')
            
            # Проверяем и резервируем товар атомарно
            cursor.execute('''
                SELECT quantity, status FROM offers 
                WHERE offer_id = ? AND status = 'active'
            ''', (offer_id,))
            offer = cursor.fetchone()
            
            if not offer or offer[0] is None or offer[0] < quantity or offer[1] != 'active':
                conn.rollback()
                return (False, None, None)
            
            current_quantity = offer[0]
            new_quantity = current_quantity - quantity
            
            # Обновляем quantity атомарно
            cursor.execute('''
                UPDATE offers 
                SET quantity = ?, status = CASE WHEN ? <= 0 THEN 'inactive' ELSE 'active' END
                WHERE offer_id = ? AND quantity = ?
            ''', (new_quantity, new_quantity, offer_id, current_quantity))
            
            if cursor.rowcount == 0:
                # Кто-то другой уже забронировал
                conn.rollback()
                return (False, None, None)
            
            # Генерируем уникальный код бронирования
            booking_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
            
            # Создаем бронирование
            cursor.execute('''
                INSERT INTO bookings (offer_id, user_id, booking_code, status, quantity)
                VALUES (?, ?, ?, 'pending', ?)
            ''', (offer_id, user_id, booking_code, quantity))
            booking_id = cursor.lastrowid
            
            # Коммитим транзакцию
            conn.commit()
            return (True, booking_id, booking_code)
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"Error in create_booking_atomic: {e}", exc_info=True)
            return (False, None, None)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    
    def get_user_bookings(self, user_id: int) -> List[Tuple]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT b.booking_id, b.offer_id, b.user_id, b.status, b.booking_code,
                       b.pickup_time, COALESCE(b.quantity, 1) as quantity, b.created_at,
                       o.title, o.discount_price, o.available_until, s.name, s.address, s.city
                FROM bookings b
                JOIN offers o ON b.offer_id = o.offer_id
                JOIN stores s ON o.store_id = s.store_id
                WHERE b.user_id = ?
                ORDER BY b.created_at DESC
            ''', (user_id,))
            bookings = cursor.fetchall()
            return bookings
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def get_booking(self, booking_id: int) -> Optional[Tuple]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            # Explicit column order to match expected structure:
            # 0:booking_id, 1:offer_id, 2:user_id, 3:status, 4:booking_code, 5:pickup_time, 6:quantity, 7:created_at
            cursor.execute('''
                SELECT booking_id, offer_id, user_id, status, booking_code, 
                       pickup_time, COALESCE(quantity, 1) as quantity, created_at 
                FROM bookings 
                WHERE booking_id = ?
            ''', (booking_id,))
            booking = cursor.fetchone()
            return booking
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def get_booking_by_code(self, booking_code: str) -> Optional[Tuple]:
        """Получить бронирование по коду"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT b.booking_id, b.offer_id, b.user_id, b.status, b.booking_code,
                       b.pickup_time, COALESCE(b.quantity, 1) as quantity, b.created_at,
                       u.first_name, u.username
                FROM bookings b
                JOIN users u ON b.user_id = u.user_id
                WHERE b.booking_code = ? AND b.status = 'pending'
            ''', (booking_code,))
            booking = cursor.fetchone()
            return booking
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def update_booking_status(self, booking_id: int, status: str):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE bookings SET status = ? WHERE booking_id = ?', (status, booking_id))
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def complete_booking(self, booking_id: int):
        """Завершить бронирование"""
        self.update_booking_status(booking_id, 'completed')
    
    def cancel_booking(self, booking_id: int):
        """Отменить бронирование"""
        self.update_booking_status(booking_id, 'cancelled')
    
    def get_store_bookings(self, store_id: int) -> List[Tuple]:
        """Получить все бронирования для магазина"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT b.*, o.title, u.first_name, u.username, u.phone
                FROM bookings b
                JOIN offers o ON b.offer_id = o.offer_id
                JOIN users u ON b.user_id = u.user_id
                WHERE o.store_id = ?
                ORDER BY b.created_at DESC
            ''', (store_id,))
            bookings = cursor.fetchall()
            return bookings
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    # Методы для админа
    def set_admin(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_admin = 1 WHERE user_id = ?', (user_id,))
        conn.commit()
        conn.close()
    
    def is_admin(self, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT is_admin FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result and result[0] == 1
    
    def get_all_admins(self) -> List[Tuple]:
        """Получить всех администраторов"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM users WHERE is_admin = 1')
        admins = cursor.fetchall()
        conn.close()
        return admins
    
    def get_pending_stores(self) -> List[Tuple]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT s.*, u.first_name, u.username
            FROM stores s
            JOIN users u ON s.owner_id = u.user_id
            WHERE s.status = 'pending'
            ORDER BY s.created_at DESC
        ''')
        stores = cursor.fetchall()
        conn.close()
        return stores
    
    def approve_store(self, store_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем данные магазина и проверяем владельца
        cursor.execute('''
            SELECT s.owner_id, u.user_id, s.status, s.name 
            FROM stores s
            LEFT JOIN users u ON s.owner_id = u.user_id
            WHERE s.store_id = ?
        ''', (store_id,))
        
        result = cursor.fetchone()
        if not result:
            conn.close()
            logger.error(f"Store {store_id} not found")
            return False
            
        owner_id, user_exists, current_status, store_name = result
        
        # Проверяем что владелец существует
        if not user_exists:
            conn.close()
            logger.error(f"Owner {owner_id} for store {store_id} ({store_name}) not found")
            return False
        
        # Проверяем что магазин еще не одобрен
        if current_status != 'pending':
            conn.close()
            logger.warning(f"Store {store_id} already has status: {current_status}")
            return False
        
        # Обновляем статус магазина
        cursor.execute('UPDATE stores SET status = ? WHERE store_id = ?', ('active', store_id))
        
        # Обновляем роль владельца
        cursor.execute('UPDATE users SET role = ? WHERE user_id = ?', ('seller', owner_id))
        
        conn.commit()
        conn.close()
        
        # Инвалидируем кэш
        try:
            cache.delete(f'store:{store_id}')
            cache.delete(f'user:{owner_id}')
        except:
            pass
        
        logger.info(f"Store {store_id} ({store_name}) approved, owner {owner_id} promoted to seller")
        return True
    
    def reject_store(self, store_id: int, reason: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE stores SET status = ?, rejection_reason = ? WHERE store_id = ?', ('rejected', reason, store_id))
        conn.commit()
        conn.close()
    
    def get_store_owner(self, store_id: int) -> Optional[int]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT owner_id FROM stores WHERE store_id = ?', (store_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    def get_statistics(self) -> dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        # Пользователи
        cursor.execute('SELECT COUNT(*) FROM users')
        stats['users'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE role = ?', ('customer',))
        stats['customers'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE role = ?', ('seller',))
        stats['sellers'] = cursor.fetchone()[0]
        
        # Магазины
        cursor.execute('SELECT COUNT(*) FROM stores')
        stats['stores'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM stores WHERE status = ?', ('active',))
        stats['approved_stores'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM stores WHERE status = ?', ('pending',))
        stats['pending_stores'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM stores WHERE status = ?', ('rejected',))
        stats['rejected_stores'] = cursor.fetchone()[0]
        
        # Предложения
        cursor.execute('SELECT COUNT(*) FROM offers')
        stats['offers'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM offers WHERE status = ?', ('active',))
        stats['active_offers'] = cursor.fetchone()[0]
        
        # Бронирования
        cursor.execute('SELECT COUNT(*) FROM bookings')
        stats['bookings'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = ?', ('pending',))
        stats['pending_bookings'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = ?', ('completed',))
        stats['completed_bookings'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM bookings WHERE status = ?', ('cancelled',))
        stats['cancelled_bookings'] = cursor.fetchone()[0]
        
        conn.close()
        return stats
    
    def get_all_users(self) -> List[Tuple]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE notifications_enabled = 1')
        users = cursor.fetchall()
        conn.close()
        return users
    
    def add_notification(self, user_id: int, message: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO notifications (user_id, message) VALUES (?, ?)', (user_id, message))
        conn.commit()
        conn.close()
    
    def toggle_notifications(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT notifications_enabled FROM users WHERE user_id = ?', (user_id,))
        current = cursor.fetchone()[0]
        new_value = 0 if current == 1 else 1
        cursor.execute('UPDATE users SET notifications_enabled = ? WHERE user_id = ?', (new_value, user_id))
        conn.commit()
        conn.close()
        return new_value == 1
    
    # Методы для рейтингов
    def add_rating(self, booking_id: int, user_id: int, store_id: int, rating: int, comment: str = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO ratings (booking_id, user_id, store_id, rating, comment)
            VALUES (?, ?, ?, ?, ?)
        ''', (booking_id, user_id, store_id, rating, comment))
        conn.commit()
        conn.close()
    
    def get_store_ratings(self, store_id: int) -> List[Tuple]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT r.*, u.first_name, u.username
            FROM ratings r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.store_id = ?
            ORDER BY r.created_at DESC
        ''', (store_id,))
        ratings = cursor.fetchall()
        conn.close()
        return ratings
    
    def get_store_average_rating(self, store_id: int) -> float:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT AVG(rating) FROM ratings WHERE store_id = ?', (store_id,))
        result = cursor.fetchone()
        conn.close()
        return round(result[0], 1) if result[0] else 0.0
    
    def has_rated_booking(self, booking_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM ratings WHERE booking_id = ?', (booking_id,))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    
    # Методы для статистики продаж
    def get_store_sales_stats(self, store_id: int) -> dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        # Всего продано
        cursor.execute('''
            SELECT COUNT(*), SUM(o.discount_price)
            FROM bookings b
            JOIN offers o ON b.offer_id = o.offer_id
            WHERE o.store_id = ? AND b.status = 'completed'
        ''', (store_id,))
        result = cursor.fetchone()
        stats['total_sales'] = result[0] if result[0] else 0
        stats['total_revenue'] = result[1] if result[1] else 0
        
        # Активные брони
        cursor.execute('''
            SELECT COUNT(*)
            FROM bookings b
            JOIN offers o ON b.offer_id = o.offer_id
            WHERE o.store_id = ? AND b.status = 'pending'
        ''', (store_id,))
        stats['pending_bookings'] = cursor.fetchone()[0]
        
        conn.close()
        return stats

    # Методы для рейтингов
    def add_rating(self, booking_id: int, user_id: int, store_id: int, rating: int, comment: str = None):
        """Добавить рейтинг"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO ratings (booking_id, user_id, store_id, rating, comment)
            VALUES (?, ?, ?, ?, ?)
        ''', (booking_id, user_id, store_id, rating, comment))
        conn.commit()
        conn.close()
    
    def get_store_ratings(self, store_id: int) -> List[Tuple]:
        """Получить все рейтинги магазина"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT r.*, u.first_name
            FROM ratings r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.store_id = ?
            ORDER BY r.created_at DESC
        ''', (store_id,))
        ratings = cursor.fetchall()
        conn.close()
        return ratings
    
    def get_store_average_rating(self, store_id: int) -> float:
        """Получить средний рейтинг магазина"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT AVG(rating) FROM ratings WHERE store_id = ?', (store_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result[0] else 0.0
    
    def has_rated_booking(self, booking_id: int) -> bool:
        """Проверить, оценил ли пользователь бронирование"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM ratings WHERE booking_id = ?', (booking_id,))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    
    # Методы для управления пользователями
    def delete_user(self, user_id: int):
        """Полное удаление пользователя и всех связанных данных"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем все магазины пользователя
        cursor.execute('SELECT store_id FROM stores WHERE owner_id = ?', (user_id,))
        stores = cursor.fetchall()
        
        # Удаляем все связанные данные для каждого магазина
        for store in stores:
            store_id = store[0]
            # Удаляем рейтинги магазина
            cursor.execute('DELETE FROM ratings WHERE store_id = ?', (store_id,))
            # Удаляем бронирования на предложения этого магазина
            cursor.execute('''
                DELETE FROM bookings 
                WHERE offer_id IN (SELECT offer_id FROM offers WHERE store_id = ?)
            ''', (store_id,))
            # Удаляем предложения магазина
            cursor.execute('DELETE FROM offers WHERE store_id = ?', (store_id,))
        
        # Удаляем магазины пользователя
        cursor.execute('DELETE FROM stores WHERE owner_id = ?', (user_id,))
        
        # Удаляем бронирования пользователя (как клиента)
        cursor.execute('DELETE FROM bookings WHERE user_id = ?', (user_id,))
        
        # Удаляем рейтинги пользователя
        cursor.execute('DELETE FROM ratings WHERE user_id = ?', (user_id,))
        
        # Удаляем уведомления пользователя
        cursor.execute('DELETE FROM notifications WHERE user_id = ?', (user_id,))
        
        # Удаляем самого пользователя
        cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        
        conn.commit()
        conn.close()
    
    # ============== НОВЫЕ МЕТОДЫ ==============
    
    # Избранное
    def add_to_favorites(self, user_id: int, store_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT INTO favorites (user_id, store_id) VALUES (?, ?)', (user_id, store_id))
            conn.commit()
        except:
            pass  # Уже в избранном
        conn.close()
    
    def remove_from_favorites(self, user_id: int, store_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM favorites WHERE user_id = ? AND store_id = ?', (user_id, store_id))
        conn.commit()
        conn.close()
    
    def get_user_favorites(self, user_id: int) -> List[Tuple]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT s.* FROM stores s
            JOIN favorites f ON s.store_id = f.store_id
            WHERE f.user_id = ?
            ORDER BY f.created_at DESC
        ''', (user_id,))
        favorites = cursor.fetchall()
        conn.close()
        return favorites
    
    def is_favorite(self, user_id: int, store_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM favorites WHERE user_id = ? AND store_id = ?', (user_id, store_id))
        result = cursor.fetchone() is not None
        conn.close()
        return result
    
    # История бронирований
    def get_booking_history(self, user_id: int, status: str = None) -> List[Tuple]:
        conn = self.get_connection()
        cursor = conn.cursor()
        if status:
            cursor.execute('''
                SELECT b.booking_id, b.offer_id, b.user_id, b.status, b.booking_code,
                       b.pickup_time, COALESCE(b.quantity, 1) as quantity, b.created_at,
                       o.title, o.discount_price, o.original_price, s.name, s.address, s.city
                FROM bookings b
                JOIN offers o ON b.offer_id = o.offer_id
                JOIN stores s ON o.store_id = s.store_id
                WHERE b.user_id = ? AND b.status = ?
                ORDER BY b.created_at DESC
            ''', (user_id, status))
        else:
            cursor.execute('''
                SELECT b.booking_id, b.offer_id, b.user_id, b.status, b.booking_code,
                       b.pickup_time, COALESCE(b.quantity, 1) as quantity, b.created_at,
                       o.title, o.discount_price, o.original_price, s.name, s.address, s.city
                FROM bookings b
                JOIN offers o ON b.offer_id = o.offer_id
                JOIN stores s ON o.store_id = s.store_id
                WHERE b.user_id = ?
                ORDER BY b.created_at DESC
            ''', (user_id,))
        history = cursor.fetchall()
        conn.close()
        return history
    
    def get_user_savings(self, user_id: int) -> float:
        """Подсчитывает сколько пользователь сэкономил"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT SUM((o.original_price - o.discount_price) * b.quantity)
            FROM bookings b
            JOIN offers o ON b.offer_id = o.offer_id
            WHERE b.user_id = ? AND b.status = 'completed'
        ''', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result and result[0] else 0
    
    # Промокоды
    def create_promo(self, code: str, discount_percent: int = 0, discount_amount: float = 0, max_uses: int = 1, valid_until: str = None):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO promocodes (code, discount_percent, discount_amount, max_uses, valid_until)
                VALUES (?, ?, ?, ?, ?)
            ''', (code, discount_percent, discount_amount, max_uses, valid_until))
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def get_promo(self, code: str) -> Optional[Tuple]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM promocodes WHERE code = ?', (code,))
            promo = cursor.fetchone()
            return promo
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def use_promo(self, user_id: int, promo_id: int, booking_id: int, discount_applied: float):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO promo_usage (user_id, promo_id, booking_id, discount_applied)
                VALUES (?, ?, ?, ?)
            ''', (user_id, promo_id, booking_id, discount_applied))
            cursor.execute('UPDATE promocodes SET current_uses = current_uses + 1 WHERE promo_id = ?', (promo_id,))
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    # Реферальная система
    def generate_referral_code(self, user_id: int) -> str:
        import random, string
        code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET referral_code = ? WHERE user_id = ?', (code, user_id))
            conn.commit()
            return code
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def use_referral(self, referrer_code: str, referred_id: int) -> bool:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT user_id FROM users WHERE referral_code = ?', (referrer_code,))
            referrer = cursor.fetchone()
            if referrer:
                cursor.execute('''
                    INSERT INTO referrals (referrer_id, referred_id, bonus_given)
                    VALUES (?, ?, 1)
                ''', (referrer[0], referred_id))
                cursor.execute('UPDATE users SET bonus_balance = bonus_balance + 5000 WHERE user_id = ?', (referrer[0],))
                cursor.execute('UPDATE users SET bonus_balance = bonus_balance + 3000 WHERE user_id = ?', (referred_id,))
                conn.commit()
                return True
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    # Логирование ошибок
    def log_error(self, error_message: str, user_id: int = None):
        import logging
        logging.basicConfig(filename='fudly_errors.log', level=logging.ERROR)
        logging.error(f"User {user_id}: {error_message}")
    
    # Бэкап базы данных
    def backup_database(self):
        import shutil
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f'fudly_backup_{timestamp}.db'
        shutil.copy2(self.db_name, backup_file)
        return backup_file

    def delete_store(self, store_id: int):
        """Полное удаление магазина и всех связанных данных"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Получаем user_id владельца магазина
            cursor.execute('SELECT owner_id FROM stores WHERE store_id = ?', (store_id,))
            result = cursor.fetchone()
            if not result:
                return
            
            user_id = result[0]
            
            # Удаляем рейтинги магазина
            cursor.execute('DELETE FROM ratings WHERE store_id = ?', (store_id,))
            
            # Удаляем бронирования на предложения этого магазина
            cursor.execute('''
                DELETE FROM bookings 
                WHERE offer_id IN (SELECT offer_id FROM offers WHERE store_id = ?)
            ''', (store_id,))
            
            # Удаляем предложения магазина
            cursor.execute('DELETE FROM offers WHERE store_id = ?', (store_id,))
            
            # Удаляем сам магазин
            cursor.execute('DELETE FROM stores WHERE store_id = ?', (store_id,))
            
            # Проверяем, остались ли у пользователя другие магазины
            cursor.execute('SELECT COUNT(*) FROM stores WHERE owner_id = ?', (user_id,))
            remaining_stores = cursor.fetchone()[0]
            
            # Если магазинов не осталось - меняем роль на customer
            if remaining_stores == 0:
                cursor.execute('UPDATE users SET role = "customer" WHERE user_id = ?', (user_id,))
            
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @staticmethod
    def get_time_remaining(expiry_date: str) -> str:
        """
        Возвращает строку с оставшимся временем до истечения срока годности
        Формат: '🕐 Годен: 2 дня' или '⏰ Срок годности истек'
        """
        if not expiry_date:
            return ""
            
        try:
            # Парсим дату истечения срока годности (формат: YYYY-MM-DD)
            if isinstance(expiry_date, str):
                if ' ' in expiry_date:
                    # Если есть время, пробуем разные форматы
                    try:
                        end_date = datetime.strptime(expiry_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            end_date = datetime.strptime(expiry_date, '%Y-%m-%d %H:%M')
                        except ValueError:
                            return ""
                elif '-' in expiry_date:
                    # Если только дата в формате YYYY-MM-DD
                    end_date = datetime.strptime(expiry_date, '%Y-%m-%d')
                elif '.' in expiry_date:
                    # Если дата в формате DD.MM.YYYY
                    end_date = datetime.strptime(expiry_date, '%d.%m.%Y')
                else:
                    return ""
            else:
                return ""
            
            current_time = datetime.now()
            
            # Если срок уже истек
            if end_date <= current_time:
                return "⏰ Срок годности истек"
                
            # Вычисляем разницу
            time_diff = end_date - current_time
            
            # Получаем дни
            days = time_diff.days
            hours, remainder = divmod(time_diff.seconds, 3600)
            
            # Формируем строку
            if days > 0:
                if days == 1:
                    return "🕐 Годен: 1 день"
                elif days < 5:
                    return f"🕐 Годен: {days} дня"
                else:
                    return f"🕐 Годен: {days} дней"
            elif hours > 0:
                if hours == 1:
                    return "🕐 Годен: 1 час"
                elif hours < 5:
                    return f"🕐 Годен: {hours} часа"
                else:
                    return f"🕐 Годен: {hours} часов"
            else:
                return "🕐 Годен: менее часа"
                
        except (ValueError, TypeError) as e:
            print(f"Error parsing expiry_date: {expiry_date}, error: {e}")
            return ""
    
    def get_stores_by_category(self, category: str, city: str = None) -> List[Tuple]:
        """Получить магазины по категории и опционально по городу"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if city:
                cursor.execute('''
                    SELECT * 
                    FROM stores 
                    WHERE category = ? AND city = ? AND status = 'active'
                    ORDER BY name
                ''', (category, city))
            else:
                cursor.execute('''
                    SELECT * 
                    FROM stores 
                    WHERE category = ? AND status = 'active'
                    ORDER BY name
                ''', (category,))
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_offers_by_store(self, store_id: int) -> List[Tuple]:
        """Получить активные предложения магазина"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT o.*, s.name, s.address, s.city, s.category
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE o.store_id = ? AND o.quantity > 0 AND date(o.expiry_date) >= date('now')
                ORDER BY o.created_at DESC
            ''', (store_id,))
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_top_offers_by_city(self, city: str, limit: int = 10) -> List[Tuple]:
        """Получить топ предложения в городе (по размеру скидки)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT o.*, s.name, s.address, s.city, s.category,
                       CAST((o.original_price - o.discount_price) AS REAL) / o.original_price * 100 as discount_percent
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE s.city = ? AND s.status = 'active' 
                      AND o.status = 'active' AND o.quantity > 0 
                      AND date(o.expiry_date) >= date('now')
                ORDER BY discount_percent DESC, o.created_at DESC
                LIMIT ?
            ''', (city, limit))
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_offers_by_city_and_category(self, city: str, category: str, limit: int = 20) -> List[Tuple]:
        """Получить предложения в городе по категории магазина"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT o.*, s.name, s.address, s.city, s.category,
                       CAST((o.original_price - o.discount_price) AS REAL) / o.original_price * 100 as discount_percent
                FROM offers o
                JOIN stores s ON o.store_id = s.store_id
                WHERE s.city = ? AND s.category = ? AND s.status = 'active'
                      AND o.status = 'active' AND o.quantity > 0 
                      AND date(o.expiry_date) >= date('now')
                ORDER BY discount_percent DESC, o.created_at DESC
                LIMIT ?
            ''', (city, category, limit))
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_stores_count_by_category(self, city: str) -> dict:
        """Получить количество магазинов по каждой категории в городе"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT category, COUNT(*) as count
                FROM stores
                WHERE city = ? AND status = 'active'
                GROUP BY category
            ''', (city,))
            results = cursor.fetchall()
            # Возвращаем словарь {категория: количество}
            return {row[0]: row[1] for row in results}
        finally:
            conn.close()
    
    def get_top_stores_by_city(self, city: str, limit: int = 10) -> List[Tuple]:
        """Получить топ магазины по рейтингу в городе"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT s.*, 
                       COALESCE(AVG(r.rating), 0) as avg_rating,
                       COUNT(r.rating_id) as ratings_count
                FROM stores s
                LEFT JOIN ratings r ON s.store_id = r.store_id
                WHERE s.city = ? AND s.status = 'active'
                GROUP BY s.store_id
                ORDER BY avg_rating DESC, ratings_count DESC
                LIMIT ?
            ''', (city, limit))
            return cursor.fetchall()
        finally:
            conn.close()

    # ============== ИЗБРАННОЕ ==============
    
    def add_favorite(self, user_id: int, store_id: int):
        """Добавить магазин в избранное"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('INSERT OR IGNORE INTO favorites (user_id, store_id) VALUES (?, ?)', 
                          (user_id, store_id))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error adding favorite: {e}")
            return False
        finally:
            conn.close()
    
    def remove_favorite(self, user_id: int, store_id: int):
        """Удалить магазин из избранного"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('DELETE FROM favorites WHERE user_id = ? AND store_id = ?', 
                          (user_id, store_id))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error removing favorite: {e}")
            return False
        finally:
            conn.close()
    
    def is_favorite(self, user_id: int, store_id: int) -> bool:
        """Проверить, в избранном ли магазин"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM favorites WHERE user_id = ? AND store_id = ?', 
                          (user_id, store_id))
            return cursor.fetchone()[0] > 0
        finally:
            conn.close()
    
    def get_favorites(self, user_id: int) -> List[Tuple]:
        """Получить избранные магазины пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT s.* FROM stores s
                JOIN favorites f ON s.store_id = f.store_id
                WHERE f.user_id = ? AND s.status = 'active'
                ORDER BY f.created_at DESC
            ''', (user_id,))
            return cursor.fetchall()
        finally:
            conn.close()

    # ============== АНАЛИТИКА ==============
    
    def get_store_analytics(self, store_id: int) -> dict:
        """Получить аналитику магазина"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Общая статистика
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_bookings,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancelled
                FROM bookings b
                JOIN offers o ON b.offer_id = o.offer_id
                WHERE o.store_id = ?
            ''', (store_id,))
            stats = cursor.fetchone()
            
            # Продажи по дням недели
            cursor.execute('''
                SELECT 
                    CAST(strftime('%w', b.created_at) AS INTEGER) as day_of_week,
                    COUNT(*) as count
                FROM bookings b
                JOIN offers o ON b.offer_id = o.offer_id
                WHERE o.store_id = ? AND b.status = 'completed'
                GROUP BY day_of_week
            ''', (store_id,))
            days = cursor.fetchall()
            
            # Популярные категории
            cursor.execute('''
                SELECT 
                    o.category,
                    COUNT(*) as count
                FROM bookings b
                JOIN offers o ON b.offer_id = o.offer_id
                WHERE o.store_id = ? AND b.status = 'completed'
                GROUP BY o.category
                ORDER BY count DESC
                LIMIT 5
            ''', (store_id,))
            categories = cursor.fetchall()
            
            # Средний рейтинг
            cursor.execute('''
                SELECT AVG(rating) as avg_rating, COUNT(*) as rating_count
                FROM ratings
                WHERE store_id = ?
            ''', (store_id,))
            rating = cursor.fetchone()
            
            return {
                'total_bookings': stats[0] or 0,
                'completed': stats[1] or 0,
                'cancelled': stats[2] or 0,
                'conversion_rate': (stats[1] / stats[0] * 100) if stats[0] > 0 else 0,
                'days_of_week': dict(days) if days else {},
                'popular_categories': categories or [],
                'avg_rating': rating[0] or 0,
                'rating_count': rating[1] or 0
            }
        finally:
            conn.close()

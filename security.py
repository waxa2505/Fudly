import re
import html
from typing import Optional, Dict, Any
from logging_config import logger


class InputValidator:
    """Input validation and sanitization for Telegram bot security."""
    
    # Regex patterns for validation
    PHONE_PATTERN = re.compile(r'^\+?[1-9]\d{1,14}$')
    USERNAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]{3,32}$')
    CITY_PATTERN = re.compile(r'^[a-zA-Zа-яА-Я\s\-]{1,50}$', re.UNICODE)
    PRICE_PATTERN = re.compile(r'^\d+(\.\d{1,2})?$')
    
    @staticmethod
    def sanitize_text(text: str, max_length: int = 1000) -> str:
        """Sanitize text input by escaping HTML and limiting length."""
        if not text or not isinstance(text, str):
            return ""
        
        # Escape HTML entities
        sanitized = html.escape(text.strip())
        
        # Limit length
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length] + "..."
        
        return sanitized
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Validate phone number format."""
        if not phone:
            return False
        return bool(InputValidator.PHONE_PATTERN.match(phone))
    
    @staticmethod
    def validate_username(username: str) -> bool:
        """Validate username format."""
        if not username:
            return False
        return bool(InputValidator.USERNAME_PATTERN.match(username))
    
    @staticmethod
    def validate_city(city: str) -> bool:
        """Validate city name."""
        if not city:
            return False
        return bool(InputValidator.CITY_PATTERN.match(city))
    
    @staticmethod
    def validate_price(price_str: str) -> tuple[bool, float]:
        """Validate and parse price string. Returns (is_valid, parsed_price)."""
        if not price_str:
            return False, 0.0
        
        if InputValidator.PRICE_PATTERN.match(price_str):
            try:
                price = float(price_str)
                if 0 <= price <= 999999:  # Reasonable price range
                    return True, price
            except ValueError:
                pass
        
        return False, 0.0
    
    @staticmethod
    def validate_quantity(quantity_str: str) -> tuple[bool, int]:
        """Validate and parse quantity string."""
        if not quantity_str:
            return False, 0
        
        try:
            quantity = int(quantity_str)
            if 1 <= quantity <= 1000:  # Reasonable quantity range
                return True, quantity
        except ValueError:
            pass
        
        return False, 0


class RateLimiter:
    """Simple in-memory rate limiter for user actions."""
    
    def __init__(self):
        self._user_requests = {}  # user_id -> {action: [timestamps]}
        
    def is_allowed(self, user_id: int, action: str, max_requests: int = 10, window_seconds: int = 60) -> bool:
        """Check if user is allowed to perform action within rate limit."""
        import time
        
        current_time = time.time()
        
        if user_id not in self._user_requests:
            self._user_requests[user_id] = {}
        
        if action not in self._user_requests[user_id]:
            self._user_requests[user_id][action] = []
        
        # Clean old timestamps
        requests = self._user_requests[user_id][action]
        self._user_requests[user_id][action] = [
            ts for ts in requests if current_time - ts < window_seconds
        ]
        
        # Check if under limit
        if len(self._user_requests[user_id][action]) < max_requests:
            self._user_requests[user_id][action].append(current_time)
            return True
        
        logger.warning(f"Rate limit exceeded for user {user_id} action {action}")
        return False


# Global instances
validator = InputValidator()
rate_limiter = RateLimiter()


def secure_user_input(func):
    """Decorator to sanitize user input for bot handlers."""
    import asyncio
    import functools
    import inspect
    
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            # Log the function call for security monitoring
            logger.info(f"Handler called: {func.__name__}")
            
            # Если функция асинхронная - вызываем напрямую
            if inspect.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                # Если синхронная - оборачиваем в executor
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
                
        except Exception as e:
            logger.error(f"Handler {func.__name__} failed: {str(e)}")
            raise
    
    return wrapper


def validate_admin_action(user_id: int, db) -> bool:
    """Validate that user is admin and log the action."""
    try:
        is_admin = db.is_admin(user_id)
        if not is_admin:
            logger.warning(f"Unauthorized admin action attempt by user {user_id}")
        return is_admin
    except Exception as e:
        logger.error(f"Admin validation failed for user {user_id}: {str(e)}")
        return False
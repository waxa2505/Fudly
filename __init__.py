"""
Handlers package - modular bot handlers using aiogram Router

This package contains modular handlers organized by functionality:
- registration: User registration (phone, city)
- user_commands: Basic commands (/start, language, city selection, cancel)
- admin: Admin panel and commands

Additional handlers remain in bot.py and can be migrated here incrementally.
"""
from handlers import registration, user_commands, admin

# Export all handler modules
__all__ = ['registration', 'user_commands', 'admin']

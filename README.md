# Handlers Package

This package contains modular bot handlers organized by functionality using `aiogram.Router`.

## Structure

The handlers are being refactored from the monolithic `bot.py` (6175 lines) into smaller, focused modules:

### Completed Modules

- **`common.py`** - Shared utilities, FSM state classes, and middleware
  - All FSM state groups (Registration, RegisterStore, CreateOffer, etc.)
  - RegistrationCheckMiddleware
  - Utility functions (has_approved_store, get_appropriate_menu, etc.)

- **`registration.py`** - User registration flow
  - process_phone - Handle phone number collection
  - process_city - Handle city selection

- **`user_commands.py`** - Basic user commands  
  - cmd_start - /start command
  - choose_language - Language selection
  - my_city - City information and change
  - cancel_action - Cancel operations
  - And related callback handlers

- **`admin.py`** - Admin panel handlers
  - cmd_admin - /admin command
  - admin_dashboard - Statistics dashboard
  - admin_exit - Exit admin panel

### Pending Migration

Additional handler groups that remain in `bot.py` and can be migrated incrementally:

- Store registration and management handlers
- Offer creation and management handlers  
- Booking handlers
- Callback handlers (pagination, filters, etc.)
- Additional admin handlers (moderation, user management, etc.)

## Usage Pattern

Each handler module follows this pattern:

```python
from aiogram import Router

router = Router()

def setup(dp_or_router, db, get_text, ...dependencies):
    """Setup handlers with dependencies"""
    
    @dp_or_router.message(...)
    async def handler_name(...):
        # Handler implementation
        pass
```

## Integration

To integrate a handler module into `bot.py`:

1. Import the module: `from handlers import module_name`
2. Call its setup function: `module_name.setup(dp, db, get_text, ...)`
3. Comment out or remove the duplicate handlers from `bot.py`

## Benefits

- **Modularity**: Handlers are organized by functionality
- **Maintainability**: Easier to find and modify specific features  
- **Testability**: Modules can be tested independently
- **Scalability**: New features can be added as new modules
- **Code Review**: Smaller files are easier to review

## Next Steps

1. Test the existing modules work correctly
2. Integrate them into `bot.py` properly
3. Gradually migrate remaining handlers
4. Remove duplicates from `bot.py`
5. Eventually make `bot.py` just initialization code

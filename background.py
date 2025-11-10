"""
Background tasks module - автоматические задачи  
Упрощённая версия для production
"""

def start_background_tasks(db):
    """Запуск фоновых задач
    
    Args:
        db: Database instance
        
    Note:
        Фоновые задачи (cleanup_expired_offers) запускаются непосредственно в bot.py
        через asyncio.create_task() после старта event loop
    """
    print("✅ Background tasks module loaded")


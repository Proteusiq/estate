#Backend Settings
CELERY_RESULT_BACKEND = "mongodb"
CELERY_MONGODB_BACKEND_SETTINGS = {
    "host": "localhost",
    "port": 27017,
    "database": "stocks", 
    "taskmeta_collection": "stocks_collection",
}
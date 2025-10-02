# -------------------
# Session / cookies
# -------------------
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False
PERMANENT_SESSION_LIFETIME = 86400
SECRET_KEY = "8dzivRln6Ozk9aZ8NVRyZYuNfF4N9oB2DHR+O2Aj/YM="

# -------------------
# Feature Flags
# -------------------
FEATURE_FLAGS = {
    'DECK_GL': True,
    'DASHBOARD_NATIVE_FILTERS': True,
}

# -------------------
# SQL Lab
# -------------------
SQLLAB_HIDE_DATABASES = ["PostgreSQL"]

# -------------------
# Map settings - WITH MAPBOX API KEY
# -------------------
MAPBOX_API_KEY = 'pk.eyJ1IjoiYWxleHRhbnNraWkiLCJhIjoiY21ldmRoOTFmMGhmMjJpczQ5MWU5YTc5ZiJ9.KyBi1s0LxcK6NYOY-7nENQ'

# Remove the blank base map setting
# DECKGL_BASE_MAP = []  # Remove this line

# -------------------
# Security headers
# -------------------
TALISMAN_ENABLED = False
CONTENT_SECURITY_POLICY_WARNING = False
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "origins": ["*"],
}
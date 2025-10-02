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

# Custom color palette that matches actual car colors
CHART_LABEL_COLORS = {
    "Black": "#1a1a1a",      # True black
    "Red": "#dc143c",        # Crimson red
    "Gray": "#808080",       # Medium gray
    "White": "#f5f5f5",      # Off-white (so it's visible on white background)
    "Green": "#228b22",      # Forest green
    "Blue": "#1e90ff",       # Dodger blue
    "Pink": "#ff69b4",       # Hot pink
    "NULL": "#d3d3d3"        # Light gray for null/unknown
}

# Make these colors available
DEFAULT_FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
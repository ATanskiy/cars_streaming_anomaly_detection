import os, constants
from dotenv import load_dotenv

load_dotenv()

# -------------------
# Session / cookies
# -------------------
SESSION_COOKIE_SAMESITE = constants.SESSION_COOKIE_SAMESITE
SESSION_COOKIE_SECURE = constants.SESSION_COOKIE_SECURE
PERMANENT_SESSION_LIFETIME = constants.PERMANENT_SESSION_LIFETIME
SECRET_KEY = os.getenv("SECRET_KEY")

# -------------------
# Feature Flags
# -------------------
FEATURE_FLAGS = constants.FEATURE_FLAGS

# -------------------
# SQL Lab
# -------------------
SQLLAB_HIDE_DATABASES = constants.SQLLAB_HIDE_DATABASES

# -------------------
# Map settings - WITH MAPBOX API KEY
# -------------------
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY")

# -------------------
# Security headers
# -------------------
TALISMAN_ENABLED = constants.TALISMAN_ENABLED
CONTENT_SECURITY_POLICY_WARNING = constants.CONTENT_SECURITY_POLICY_WARNING
ENABLE_CORS = constants.ENABLE_CORS
CORS_OPTIONS = constants.CORS_OPTIONS
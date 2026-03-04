PLATFORMS = [
    "facebook",
    "instagram",
    "youtube",
    "linkedin",
    "twitter",
    "google_reviews",
]

PLATFORM_LABELS = {
    "facebook": "Facebook",
    "instagram": "Instagram",
    "youtube": "YouTube",
    "linkedin": "LinkedIn",
    "twitter": "X / Twitter",
    "google_reviews": "Google Reviews",
}

PLATFORM_COLORS = {
    "facebook": "#1877F2",
    "instagram": "#E4405F",
    "youtube": "#FF0000",
    "linkedin": "#0A66C2",
    "twitter": "#000000",
    "google_reviews": "#4285F4",
}

SENTIMENT_TYPES = ["positive", "negative", "neutral", "lead", "business"]

SENTIMENT_COLORS = {
    "positive": "#10b981",
    "negative": "#f43f5e",
    "neutral": "#6b7280",
    "lead": "#3b82f6",
    "business": "#8b5cf6",
}

SENTIMENT_BG = {
    "positive": "bg-emerald-100 text-emerald-700",
    "negative": "bg-rose-100 text-rose-700",
    "neutral": "bg-gray-100 text-gray-700",
    "lead": "bg-blue-100 text-blue-700",
    "business": "bg-purple-100 text-purple-700",
}

MEDIA_TYPES = ["image", "video", "carousel", "text"]

# Permission matrix modules and their actions
PERMISSION_MODULES = {
    "dashboard": ["view"],
    "posts": ["view", "create", "delete"],
    "comments": ["view", "reply", "delete", "translate"],
    "contacts": ["view", "export"],
    "analytics": ["view"],
    "users": ["view", "create", "delete"],
    "pages": ["view", "connect", "disconnect"],
    "settings": ["view", "edit"],
}

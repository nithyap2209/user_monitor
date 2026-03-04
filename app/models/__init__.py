from app.models.company import Company
from app.models.designation import Designation
from app.models.user import User
from app.models.company_api_key import CompanyAPIKey
from app.models.connected_page import ConnectedPage
from app.models.post import Post
from app.models.comment import Comment
from app.models.contact import Contact
from app.models.post_reaction import PostReaction


def register_models():
    """Import all models so SQLAlchemy knows about them."""
    pass  # Imports above are sufficient


__all__ = [
    "Company",
    "Designation",
    "User",
    "CompanyAPIKey",
    "ConnectedPage",
    "Post",
    "Comment",
    "Contact",
    "PostReaction",
]

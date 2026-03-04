from app.extensions import db
from datetime import datetime, timezone


class Comment(db.Model):
    __tablename__ = "comments"

    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey("posts.id"), nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=False)
    platform = db.Column(db.String(50), nullable=False)
    platform_comment_id = db.Column(db.String(300))

    # Author
    platform_author_id = db.Column(db.String(300))  # Facebook/Instagram/YouTube user ID
    author_name = db.Column(db.String(300))
    author_profile_url = db.Column(db.String(500))
    author_avatar_url = db.Column(db.String(500))

    # Content
    comment_text = db.Column(db.Text, nullable=False)
    likes_count = db.Column(db.Integer, default=0)
    parent_comment_id = db.Column(db.Integer, db.ForeignKey("comments.id"), nullable=True)

    # AI/NLP
    sentiment = db.Column(db.String(20))  # positive, negative, neutral, lead, business
    sentiment_score = db.Column(db.Float)
    detected_language = db.Column(db.String(50))
    translated_text = db.Column(db.Text)
    target_language = db.Column(db.String(10))  # language code the text was translated to
    source_language = db.Column(db.String(10))  # source language code used for translation
    keywords = db.Column(db.JSON)
    has_contact_info = db.Column(db.Boolean, default=False)

    # Reply tracking
    is_replied = db.Column(db.Boolean, default=False)
    replied_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    replied_at = db.Column(db.DateTime)
    reply_text = db.Column(db.Text)

    # Status
    is_hidden = db.Column(db.Boolean, default=False)
    is_flagged = db.Column(db.Boolean, default=False)
    is_deleted = db.Column(db.Boolean, default=False)

    # Timestamps
    commented_at = db.Column(db.DateTime)
    synced_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Self-referential for replies
    replies = db.relationship(
        "Comment",
        backref=db.backref("parent", remote_side="Comment.id"),
        lazy="dynamic",
    )
    replier = db.relationship("User", foreign_keys=[replied_by], backref="replied_comments")

    @property
    def display_name(self):
        """Return author_name if available, otherwise platform author ID."""
        if self.author_name and self.author_name != "Unknown":
            return self.author_name
        if self.platform_author_id:
            return f"User {self.platform_author_id}"
        return "Unknown"

    @property
    def sentiment_color(self):
        return {
            "positive": "emerald",
            "negative": "rose",
            "neutral": "gray",
            "lead": "blue",
            "business": "purple",
        }.get(self.sentiment, "gray")

    def __repr__(self):
        return f"<Comment {self.platform}:{self.platform_comment_id}>"

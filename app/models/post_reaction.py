from app.extensions import db
from datetime import datetime, timezone


class PostReaction(db.Model):
    __tablename__ = "post_reactions"

    id = db.Column(db.Integer, primary_key=True)
    post_id = db.Column(db.Integer, db.ForeignKey("posts.id"), nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=False)
    platform = db.Column(db.String(50), nullable=False)
    platform_user_id = db.Column(db.String(300))
    user_name = db.Column(db.String(300))
    reaction_type = db.Column(db.String(50), default="like")  # LIKE, LOVE, HAHA, WOW, SAD, ANGRY
    reacted_at = db.Column(db.DateTime)
    synced_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    post = db.relationship(
        "Post",
        backref=db.backref("reactions", lazy="dynamic", cascade="all, delete-orphan"),
    )

    def __repr__(self):
        return f"<PostReaction {self.platform}:{self.user_name}:{self.reaction_type}>"

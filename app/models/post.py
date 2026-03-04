from app.extensions import db
from datetime import datetime, timezone


class Post(db.Model):
    __tablename__ = "posts"

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=False)
    connected_page_id = db.Column(db.Integer, db.ForeignKey("connected_pages.id"), nullable=False)
    platform = db.Column(db.String(50), nullable=False)
    platform_post_id = db.Column(db.String(300))
    caption = db.Column(db.Text)
    media_url = db.Column(db.String(1000))
    media_type = db.Column(db.String(50))  # image, video, carousel, text
    thumbnail_url = db.Column(db.String(500))
    permalink = db.Column(db.String(500))
    likes_count = db.Column(db.Integer, default=0)
    comments_count = db.Column(db.Integer, default=0)
    shares_count = db.Column(db.Integer, default=0)
    reach = db.Column(db.Integer, default=0)
    views = db.Column(db.Integer, default=0)
    posted_at = db.Column(db.DateTime)
    synced_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    comments = db.relationship("Comment", backref="post", lazy="dynamic", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Post {self.platform}:{self.platform_post_id}>"

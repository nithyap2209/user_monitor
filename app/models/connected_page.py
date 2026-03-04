from app.extensions import db
from datetime import datetime, timezone


class ConnectedPage(db.Model):
    __tablename__ = "connected_pages"

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=False)
    platform = db.Column(db.String(50), nullable=False)
    page_name = db.Column(db.String(300))
    page_id = db.Column(db.String(300))
    access_token = db.Column(db.Text)
    refresh_token = db.Column(db.Text)
    token_expires_at = db.Column(db.DateTime)
    status = db.Column(db.String(20), default="connected")
    followers_count = db.Column(db.Integer, default=0)
    page_url = db.Column(db.String(500))
    page_avatar = db.Column(db.String(500))
    last_synced_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    posts = db.relationship("Post", backref="connected_page", lazy="dynamic", cascade="all, delete-orphan")

    @property
    def status_color(self):
        return {
            "connected": "emerald",
            "disconnected": "rose",
            "expired": "amber",
        }.get(self.status, "gray")

    def __repr__(self):
        return f"<ConnectedPage {self.platform}:{self.page_name}>"

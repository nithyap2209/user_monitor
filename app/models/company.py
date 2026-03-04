from app.extensions import db
from datetime import datetime, timezone


class Company(db.Model):
    __tablename__ = "companies"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    slug = db.Column(db.String(200), unique=True)
    logo_url = db.Column(db.String(500))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    users = db.relationship("User", backref="company", lazy="dynamic")
    api_keys = db.relationship("CompanyAPIKey", backref="company", lazy="dynamic", cascade="all, delete-orphan")
    connected_pages = db.relationship("ConnectedPage", backref="company", lazy="dynamic", cascade="all, delete-orphan")
    posts = db.relationship("Post", backref="company", lazy="dynamic", cascade="all, delete-orphan")
    comments = db.relationship("Comment", backref="company", lazy="dynamic", cascade="all, delete-orphan")
    contacts = db.relationship("Contact", backref="company", lazy="dynamic", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Company {self.name}>"

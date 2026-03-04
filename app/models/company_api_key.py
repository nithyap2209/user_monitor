from app.extensions import db
from datetime import datetime, timezone
from app.utils.constants import PLATFORMS


class CompanyAPIKey(db.Model):
    """Stores API credentials per company per platform.

    Each company can have one set of API keys per platform (facebook, instagram,
    youtube, linkedin, twitter, google_reviews).  The sync engine and service
    layers look up credentials via company_id + platform so that all fetched
    data is automatically scoped to the owning company.
    """

    __tablename__ = "company_api_keys"
    __table_args__ = (
        db.UniqueConstraint("company_id", "platform", name="uq_company_platform"),
    )

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=False)
    platform = db.Column(db.String(50), nullable=False)  # facebook, instagram, youtube, etc.

    # Common OAuth fields
    access_token = db.Column(db.Text)
    refresh_token = db.Column(db.Text)
    token_expires_at = db.Column(db.DateTime)

    # Platform-specific fields
    api_key = db.Column(db.Text)           # e.g. YouTube Data API key
    api_secret = db.Column(db.Text)        # e.g. app secret
    page_id = db.Column(db.String(200))    # e.g. Facebook page ID
    extra_data = db.Column(db.JSON)        # Any additional config

    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    @classmethod
    def get_for_company(cls, company_id, platform):
        """Retrieve active API key for a company + platform pair."""
        return cls.query.filter_by(
            company_id=company_id, platform=platform, is_active=True
        ).first()

    def __repr__(self):
        return f"<CompanyAPIKey company={self.company_id} platform={self.platform}>"

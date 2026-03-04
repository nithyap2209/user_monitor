from app.extensions import db
from flask_login import UserMixin
from datetime import datetime, timezone
import bcrypt


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(200), nullable=False)
    email = db.Column(db.String(200), unique=True, nullable=False)
    password_hash = db.Column(db.String(500), nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=False)
    designation_id = db.Column(db.Integer, db.ForeignKey("designations.id"), nullable=False)
    avatar_url = db.Column(db.String(500))
    is_active = db.Column(db.Boolean, default=True)
    email_verified = db.Column(db.Boolean, default=False)
    verification_token = db.Column(db.String(200))
    password_reset_token = db.Column(db.String(200))
    last_login = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def set_password(self, password):
        self.password_hash = bcrypt.hashpw(
            password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

    def check_password(self, password):
        return bcrypt.checkpw(
            password.encode("utf-8"), self.password_hash.encode("utf-8")
        )

    def has_permission(self, module, action="view"):
        if self.designation and self.designation.slug == "super_admin":
            return True
        if self.designation:
            return self.designation.has_permission(module, action)
        return False

    @property
    def is_superadmin(self):
        return self.designation and self.designation.slug == "super_admin"

    @property
    def initials(self):
        parts = self.full_name.split()
        if len(parts) >= 2:
            return (parts[0][0] + parts[-1][0]).upper()
        return self.full_name[0].upper() if self.full_name else "?"

    def __repr__(self):
        return f"<User {self.email}>"

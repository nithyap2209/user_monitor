from app.extensions import db
from datetime import datetime, timezone

# Default permission structure for reference
DEFAULT_PERMISSIONS_TEMPLATE = {
    "dashboard": {"view": False},
    "posts": {"view": False, "create": False, "delete": False},
    "comments": {"view": False, "reply": False, "delete": False, "translate": False},
    "contacts": {"view": False, "export": False},
    "analytics": {"view": False},
    "users": {"view": False, "create": False, "delete": False},
    "pages": {"view": False, "connect": False, "disconnect": False},
    "settings": {"view": False, "edit": False},
}


class Designation(db.Model):
    __tablename__ = "designations"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    slug = db.Column(db.String(100), unique=True)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=True)
    permissions = db.Column(db.JSON, default=dict)
    is_system = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    users = db.relationship("User", backref="designation", lazy="dynamic")

    def has_permission(self, module, action="view"):
        if not self.permissions:
            return False
        module_perms = self.permissions.get(module, {})
        return module_perms.get(action, False)

    def __repr__(self):
        return f"<Designation {self.name}>"

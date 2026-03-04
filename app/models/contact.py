from app.extensions import db
from datetime import datetime, timezone


class Contact(db.Model):
    __tablename__ = "contacts"

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey("companies.id"), nullable=False)
    comment_id = db.Column(db.Integer, db.ForeignKey("comments.id"), nullable=True)
    source_post_id = db.Column(db.Integer, db.ForeignKey("posts.id"), nullable=True)

    name = db.Column(db.String(300))
    phone = db.Column(db.String(50))
    email = db.Column(db.String(200))
    platform = db.Column(db.String(50))
    contact_type = db.Column(db.String(20))  # lead, business, manual
    notes = db.Column(db.Text)
    is_contacted = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    comment = db.relationship("Comment", backref="extracted_contacts")
    source_post = db.relationship("Post", backref="extracted_contacts")

    def __repr__(self):
        return f"<Contact {self.name or self.email or self.phone}>"

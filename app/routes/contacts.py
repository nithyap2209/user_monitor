from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from app.extensions import db
from app.models.contact import Contact
from app.utils.helpers import get_pagination_args, export_csv
from app.utils.decorators import permission_required

contacts_bp = Blueprint("contacts", __name__, url_prefix="/contacts")


@contacts_bp.route("/")
@login_required
@permission_required("contacts")
def index():
    page, per_page = get_pagination_args()
    company_id = current_user.company_id

    query = Contact.query.filter_by(company_id=company_id)

    search = request.args.get("q")
    if search:
        query = query.filter(
            db.or_(
                Contact.name.ilike(f"%{search}%"),
                Contact.email.ilike(f"%{search}%"),
                Contact.phone.ilike(f"%{search}%"),
            )
        )

    contact_type = request.args.get("type")
    if contact_type:
        query = query.filter_by(contact_type=contact_type)

    query = query.order_by(Contact.created_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    return render_template("contacts/index.html", contacts=pagination.items, pagination=pagination)


@contacts_bp.route("/add", methods=["POST"])
@login_required
@permission_required("contacts")
def add():
    contact = Contact(
        company_id=current_user.company_id,
        name=request.form.get("name", "").strip(),
        email=request.form.get("email", "").strip(),
        phone=request.form.get("phone", "").strip(),
        contact_type="manual",
    )
    db.session.add(contact)
    db.session.commit()
    flash("Contact added.", "success")
    return redirect(url_for("contacts.index"))


@contacts_bp.route("/delete/<int:contact_id>", methods=["POST"])
@login_required
@permission_required("contacts")
def delete(contact_id):
    contact = Contact.query.get_or_404(contact_id)
    if contact.company_id != current_user.company_id:
        flash("Unauthorized.", "danger")
        return redirect(url_for("contacts.index"))
    db.session.delete(contact)
    db.session.commit()
    flash("Contact deleted.", "success")
    return redirect(url_for("contacts.index"))


@contacts_bp.route("/export")
@login_required
@permission_required("contacts", "export")
def export():
    company_id = current_user.company_id
    contacts = Contact.query.filter_by(company_id=company_id).order_by(Contact.created_at.desc()).all()

    headers = ["ID", "Name", "Email", "Phone", "Platform", "Type", "Contacted", "Date"]
    rows = [
        [
            c.id,
            c.name or "",
            c.email or "",
            c.phone or "",
            c.platform or "",
            c.contact_type or "",
            "Yes" if c.is_contacted else "No",
            c.created_at.strftime("%Y-%m-%d"),
        ]
        for c in contacts
    ]

    return export_csv(rows, headers, filename="contacts_export.csv")

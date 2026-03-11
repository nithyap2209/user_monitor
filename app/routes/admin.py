import re
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app.extensions import db
from app.models.user import User
from app.models.company import Company
from app.models.company_api_key import CompanyAPIKey
from app.models.designation import Designation
from app.utils.decorators import role_required
from app.utils.constants import PLATFORMS, PLATFORM_LABELS, PERMISSION_MODULES

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


# ── Companies ──────────────────────────────────────────────

@admin_bp.route("/companies")
@login_required
@role_required("super_admin", "admin")
def companies():
    companies_list = Company.query.order_by(Company.name).all()
    designations_list = Designation.query.order_by(Designation.name).all()
    return render_template(
        "admin/companies.html",
        companies=companies_list,
        designations=designations_list,
    )


@admin_bp.route("/companies/add", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def add_company():
    name = request.form.get("name", "").strip()
    if not name:
        flash("Company name is required.", "danger")
        return redirect(url_for("admin.companies"))

    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    company = Company(name=name, slug=slug)
    db.session.add(company)
    db.session.commit()
    flash(f"Company '{name}' created.", "success")
    return redirect(url_for("admin.companies"))


@admin_bp.route("/companies/<int:company_id>/api-keys")
@login_required
@role_required("super_admin", "admin")
def company_api_keys(company_id):
    company = Company.query.get_or_404(company_id)
    keys = CompanyAPIKey.query.filter_by(company_id=company_id).all()
    return render_template(
        "admin/api_keys.html",
        company=company,
        keys=keys,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
    )


@admin_bp.route("/companies/<int:company_id>/api-keys/save", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def save_api_key(company_id):
    Company.query.get_or_404(company_id)
    platform = request.form.get("platform")

    if platform not in PLATFORMS:
        flash("Invalid platform.", "danger")
        return redirect(url_for("admin.company_api_keys", company_id=company_id))

    key = CompanyAPIKey.query.filter_by(company_id=company_id, platform=platform).first()
    if not key:
        key = CompanyAPIKey(company_id=company_id, platform=platform)
        db.session.add(key)

    key.access_token = request.form.get("access_token", "").strip() or None
    key.refresh_token = request.form.get("refresh_token", "").strip() or None
    key.api_key = request.form.get("api_key", "").strip() or None
    key.api_secret = request.form.get("api_secret", "").strip() or None
    key.page_id = request.form.get("page_id", "").strip() or None
    key.is_active = True

    # Handle YouTube OAuth Client ID (stored in extra_data JSON)
    oauth_client_id = request.form.get("oauth_client_id", "").strip()
    if platform == "youtube" and oauth_client_id:
        extra = key.extra_data or {}
        extra["oauth_client_id"] = oauth_client_id
        key.extra_data = extra
    elif platform == "youtube" and not oauth_client_id:
        extra = key.extra_data or {}
        extra.pop("oauth_client_id", None)
        key.extra_data = extra if extra else None

    db.session.commit()
    flash(f"{PLATFORM_LABELS.get(platform, platform)} API key saved.", "success")
    return redirect(url_for("admin.company_api_keys", company_id=company_id))


@admin_bp.route("/companies/<int:company_id>/api-keys/test", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def test_api_key(company_id):
    """Test a platform's API key by making a real API call."""
    Company.query.get_or_404(company_id)
    data = request.get_json(silent=True) or {}
    platform = data.get("platform")

    if platform not in PLATFORMS:
        return jsonify({"success": False, "error": "Invalid platform."})

    from app.services.sync_engine import SERVICE_MAP
    service_cls = SERVICE_MAP.get(platform)
    if not service_cls:
        return jsonify({"success": False, "error": f"No service for {platform}."})

    service = service_cls(company_id)
    if not service.is_configured:
        return jsonify({"success": False, "error": "API keys not configured. Save your keys first."})

    result = service.test_connection()
    return jsonify(result)


# ── Users ──────────────────────────────────────────────────

@admin_bp.route("/users")
@login_required
@role_required("super_admin", "admin")
def users():
    users_list = User.query.order_by(User.created_at.desc()).all()
    companies_list = Company.query.order_by(Company.name).all()
    designations_list = Designation.query.order_by(Designation.name).all()
    return render_template(
        "admin/users.html",
        users=users_list,
        companies=companies_list,
        designations=designations_list,
    )


@admin_bp.route("/users/add", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def add_user():
    email = request.form.get("email", "").strip()
    full_name = request.form.get("full_name", "").strip()
    password = request.form.get("password", "")
    company_id = request.form.get("company_id", type=int)
    designation_id = request.form.get("designation_id", type=int)

    if User.query.filter_by(email=email).first():
        flash("Email already exists.", "danger")
        return redirect(url_for("admin.users"))

    email_verified = "email_verified" in request.form

    user = User(
        email=email,
        full_name=full_name,
        company_id=company_id,
        designation_id=designation_id,
        email_verified=email_verified,
    )
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    flash(f"User '{email}' created.", "success")
    if request.form.get("redirect_to") == "companies":
        return redirect(url_for("admin.companies"))
    return redirect(url_for("admin.users"))


# ── Designations ───────────────────────────────────────────

@admin_bp.route("/designations")
@login_required
@role_required("super_admin", "admin")
def designations():
    designations_list = Designation.query.order_by(Designation.name).all()
    return render_template(
        "admin/designations.html",
        designations=designations_list,
        permission_modules=PERMISSION_MODULES,
    )


@admin_bp.route("/designations/add", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def add_designation():
    name = request.form.get("name", "").strip()
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

    if Designation.query.filter_by(slug=slug).first():
        flash("Designation already exists.", "danger")
        return redirect(url_for("admin.designations"))

    # Build permissions JSON from checkbox matrix
    permissions = {}
    for module, actions in PERMISSION_MODULES.items():
        module_perms = {}
        for action in actions:
            field_name = f"perm_{module}_{action}"
            module_perms[action] = field_name in request.form
        permissions[module] = module_perms

    designation = Designation(name=name, slug=slug, permissions=permissions)
    db.session.add(designation)
    db.session.commit()
    flash(f"Designation '{name}' created.", "success")
    return redirect(url_for("admin.designations"))


@admin_bp.route("/designations/<int:designation_id>/edit", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def edit_designation(designation_id):
    designation = Designation.query.get_or_404(designation_id)
    name = request.form.get("name", "").strip()

    if name and name != designation.name:
        new_slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        existing = Designation.query.filter(
            Designation.slug == new_slug, Designation.id != designation_id
        ).first()
        if existing:
            flash("A designation with that name already exists.", "danger")
            return redirect(url_for("admin.designations"))
        designation.name = name
        designation.slug = new_slug

    # Rebuild permissions from checkbox matrix
    permissions = {}
    for module, actions in PERMISSION_MODULES.items():
        module_perms = {}
        for action in actions:
            field_name = f"perm_{module}_{action}"
            module_perms[action] = field_name in request.form
        permissions[module] = module_perms
    designation.permissions = permissions

    db.session.commit()
    flash(f"Designation '{designation.name}' updated.", "success")
    return redirect(url_for("admin.designations"))


@admin_bp.route("/designations/<int:designation_id>/delete", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def delete_designation(designation_id):
    designation = Designation.query.get_or_404(designation_id)

    if designation.is_system:
        flash("System designations cannot be deleted.", "danger")
        return redirect(url_for("admin.designations"))

    if designation.users.count() > 0:
        flash(f"Cannot delete '{designation.name}' — it has {designation.users.count()} user(s) assigned.", "danger")
        return redirect(url_for("admin.designations"))

    name = designation.name
    db.session.delete(designation)
    db.session.commit()
    flash(f"Designation '{name}' deleted.", "success")
    return redirect(url_for("admin.designations"))


# ── User Edit & Delete ────────────────────────────────────

@admin_bp.route("/users/<int:user_id>/edit", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def edit_user(user_id):
    user = User.query.get_or_404(user_id)
    full_name = request.form.get("full_name", "").strip()
    email = request.form.get("email", "").strip()
    password = request.form.get("password", "").strip()
    company_id = request.form.get("company_id", type=int)
    designation_id = request.form.get("designation_id", type=int)

    redirect_target = url_for("admin.companies") if request.form.get("redirect_to") == "companies" else url_for("admin.users")

    if full_name:
        user.full_name = full_name
    if email and email != user.email:
        if User.query.filter_by(email=email).first():
            flash("Email already exists.", "danger")
            return redirect(redirect_target)
        user.email = email
    if password:
        user.set_password(password)
    if company_id:
        user.company_id = company_id
    if designation_id:
        user.designation_id = designation_id

    user.email_verified = "email_verified" in request.form

    db.session.commit()
    flash(f"User '{user.email}' updated.", "success")
    return redirect(redirect_target)


@admin_bp.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
@role_required("super_admin", "admin")
def delete_user(user_id):
    user = User.query.get_or_404(user_id)
    if user.id == current_user.id:
        flash("You cannot delete your own account.", "danger")
        return redirect(url_for("admin.users"))

    email = user.email
    db.session.delete(user)
    db.session.commit()
    flash(f"User '{email}' deleted.", "success")
    if request.form.get("redirect_to") == "companies":
        return redirect(url_for("admin.companies"))
    return redirect(url_for("admin.users"))

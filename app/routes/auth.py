import re
from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, login_required, current_user
from app.extensions import db
from app.models.user import User
from app.models.company import Company
from app.models.designation import Designation
from app.services.email_service import send_verification_email, send_password_reset_email
from datetime import datetime, timezone

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.index"))

    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")

        user = User.query.filter_by(email=email).first()
        if user and user.check_password(password):
            if not user.is_active:
                flash("Your account has been deactivated.", "danger")
                return redirect(url_for("auth.login"))
            if not user.email_verified:
                return redirect(url_for("auth.verify_email_page", email=email))
            login_user(user, remember=request.form.get("remember"))
            try:
                user.last_login = datetime.now(timezone.utc)
                db.session.commit()
            except Exception:
                db.session.rollback()
            next_page = request.args.get("next")
            return redirect(next_page or url_for("dashboard.index"))
        flash("Invalid email or password.", "danger")

    return render_template("auth/login.html")


@auth_bp.route("/signup", methods=["GET", "POST"])
def signup():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.index"))

    companies = Company.query.filter_by(is_active=True).order_by(Company.name).all()
    designations = Designation.query.filter(Designation.slug != "super_admin").order_by(Designation.name).all()

    if request.method == "POST":
        email = request.form.get("email", "").strip()
        full_name = request.form.get("full_name", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        company_id = request.form.get("company", type=int)
        designation_id = request.form.get("designation", type=int)

        if not full_name:
            flash("Full name is required.", "danger")
            return redirect(url_for("auth.signup"))

        if not _EMAIL_RE.match(email):
            flash("Please enter a valid email address.", "danger")
            return redirect(url_for("auth.signup"))

        if not company_id:
            flash("Please select a company.", "danger")
            return redirect(url_for("auth.signup"))

        if password != confirm:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("auth.signup"))

        if len(password) < 6:
            flash("Password must be at least 6 characters.", "danger")
            return redirect(url_for("auth.signup"))

        if not re.search(r"[A-Za-z]", password) or not re.search(r"[0-9]", password) or not re.search(r"[^A-Za-z0-9]", password):
            flash("Password must contain letters, numbers, and a special character.", "danger")
            return redirect(url_for("auth.signup"))

        if User.query.filter_by(email=email).first():
            flash("Email already registered.", "danger")
            return redirect(url_for("auth.signup"))

        # Default to viewer designation if none selected
        if not designation_id:
            viewer = Designation.query.filter_by(slug="viewer").first()
            designation_id = viewer.id if viewer else None

        user = User(
            email=email,
            full_name=full_name,
            company_id=company_id,
            designation_id=designation_id,
            email_verified=False,
        )
        user.set_password(password)
        db.session.add(user)
        db.session.commit()

        if send_verification_email(user):
            flash("Account created! A verification email has been sent to your inbox.", "success")
        else:
            flash("Account created but we couldn't send the verification email. Please contact admin.", "warning")
        return redirect(url_for("auth.login"))

    return render_template("auth/signup.html", companies=companies, designations=designations)


@auth_bp.route("/verify/<token>")
def verify_email(token):
    if not token:
        flash("Invalid verification link.", "danger")
        return redirect(url_for("auth.login"))

    user = User.query.filter_by(verification_token=token).first()
    if not user:
        flash("Invalid or expired verification link.", "danger")
        return redirect(url_for("auth.login"))

    user.email_verified = True
    user.verification_token = None
    db.session.commit()
    flash("Email verified successfully! You can now log in.", "success")
    return redirect(url_for("auth.login"))


@auth_bp.route("/verify-email", methods=["GET", "POST"])
def verify_email_page():
    email = request.args.get("email", "").strip() if request.method == "GET" else ""

    if request.method == "POST":
        current_email = request.form.get("current_email", "").strip()
        new_email = request.form.get("new_email", "").strip()

        user = User.query.filter_by(email=current_email).first()
        if not user or user.email_verified:
            flash("Invalid request.", "danger")
            return redirect(url_for("auth.login"))

        # If email changed, validate and update
        if new_email and new_email != current_email:
            if not _EMAIL_RE.match(new_email):
                flash("Please enter a valid email address.", "danger")
                return redirect(url_for("auth.verify_email_page", email=current_email))
            if User.query.filter_by(email=new_email).first():
                flash("This email is already registered.", "danger")
                return redirect(url_for("auth.verify_email_page", email=current_email))
            user.email = new_email
            db.session.commit()

        if send_verification_email(user):
            flash("Verification email sent to " + user.email + ". Check your inbox.", "success")
        else:
            flash("Failed to send email. Please try again later.", "danger")
        return redirect(url_for("auth.verify_email_page", email=user.email))

    return render_template("auth/verify_email.html", email=email)


@auth_bp.route("/forgot", methods=["GET", "POST"])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.index"))

    if request.method == "POST":
        email = request.form.get("email", "").strip()
        user = User.query.filter_by(email=email).first()
        if user:
            if send_password_reset_email(user):
                flash("Password reset link sent to your email.", "success")
            else:
                flash("Failed to send email. Please try again later.", "danger")
        else:
            # Don't reveal whether email exists
            flash("If that email is registered, a reset link has been sent.", "info")
        return redirect(url_for("auth.login"))

    return render_template("auth/forgot_password.html")


@auth_bp.route("/reset/<token>", methods=["GET", "POST"])
def reset_password(token):
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.index"))

    user = User.query.filter_by(password_reset_token=token).first()
    if not user:
        flash("Invalid or expired reset link.", "danger")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")

        if not password or len(password) < 6:
            flash("Password must be at least 6 characters.", "danger")
            return redirect(url_for("auth.reset_password", token=token))

        if password != confirm:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("auth.reset_password", token=token))

        user.set_password(password)
        user.password_reset_token = None
        db.session.commit()
        flash("Password reset successfully! You can now log in.", "success")
        return redirect(url_for("auth.login"))

    return render_template("auth/reset_password.html", token=token)


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("auth.login"))

"""Public pages — no authentication required.

Serves robots.txt, privacy policy, terms of service, cookie policy,
and the public landing / home page.
"""

from flask import Blueprint, render_template, send_from_directory, current_app

public_bp = Blueprint("public", __name__)


@public_bp.route("/robots.txt")
def robots():
    return send_from_directory(current_app.static_folder, "robots.txt")


@public_bp.route("/home")
def home():
    return render_template("public/home.html")


@public_bp.route("/privacy")
def privacy():
    return render_template("public/privacy.html")


@public_bp.route("/terms")
def terms():
    return render_template("public/terms.html")


@public_bp.route("/cookies")
def cookies():
    return render_template("public/cookies.html")

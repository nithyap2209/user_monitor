import os
from flask import Flask
from flask_login import current_user
from dotenv import load_dotenv

load_dotenv()


def create_app(config_name=None):
    app = Flask(__name__)

    if config_name is None:
        config_name = os.getenv("FLASK_ENV", "development")

    from app.config import config_map
    app.config.from_object(config_map.get(config_name, config_map["development"]))

    # Initialize extensions
    from app.extensions import db, login_manager, migrate, csrf, celery, mail
    db.init_app(app)

    # Enable WAL mode for SQLite to allow concurrent reads during writes
    if app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite"):
        from sqlalchemy import event

        def _set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()

        with app.app_context():
            event.listen(db.engine, "connect", _set_sqlite_pragma)

    login_manager.init_app(app)
    migrate.init_app(app, db)
    csrf.init_app(app)
    mail.init_app(app)

    # Configure Celery
    celery.conf.broker_url = app.config["CELERY_BROKER_URL"]
    celery.conf.result_backend = app.config["CELERY_RESULT_BACKEND"]
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask

    # Register blueprints
    from app.routes.auth import auth_bp
    from app.routes.dashboard import dashboard_bp
    from app.routes.pages import pages_bp
    from app.routes.posts import posts_bp
    from app.routes.comments import comments_bp
    from app.routes.inbox import inbox_bp
    from app.routes.contacts import contacts_bp
    from app.routes.admin import admin_bp
    from app.routes.api import api_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(pages_bp)
    app.register_blueprint(posts_bp)
    app.register_blueprint(comments_bp)
    app.register_blueprint(inbox_bp)
    app.register_blueprint(contacts_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(api_bp)

    # User loader
    from app.models.user import User

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    # Jinja2 context processor — inject permission checker + helpers
    @app.context_processor
    def inject_helpers():
        def has_permission(module, action="view"):
            if not current_user.is_authenticated:
                return False
            return current_user.has_permission(module, action)

        def profile_url(platform, author_id=None, author_name=None):
            """Build a social media profile URL from platform + user identifier."""
            if platform == "facebook" and author_id:
                return f"https://facebook.com/{author_id}"
            if platform == "instagram" and (author_name or author_id):
                return f"https://instagram.com/{author_name or author_id}"
            if platform == "youtube" and author_id:
                return f"https://youtube.com/channel/{author_id}"
            if platform == "twitter" and (author_name or author_id):
                return f"https://x.com/{author_name or author_id}"
            if platform == "linkedin" and (author_name or author_id):
                return f"https://linkedin.com/in/{author_name or author_id}"
            return None

        return dict(has_permission=has_permission, profile_url=profile_url)

    # Custom Jinja2 filters
    import re as _re
    _date_like = _re.compile(r"^\d{1,4}[-/]\d{1,2}[-/]\d{2,4}")

    @app.template_filter("clean_phone")
    def clean_phone_filter(phone):
        """Return phone if valid, else empty string (filters out date-like values)."""
        if not phone:
            return ""
        phone = phone.strip()
        if _date_like.match(phone):
            return ""
        digits = _re.sub(r"\D", "", phone)
        return phone if len(digits) >= 7 else ""

    # Root redirect
    from flask import redirect, url_for

    @app.route("/")
    def index():
        if current_user.is_authenticated:
            return redirect(url_for("dashboard.index"))
        return redirect(url_for("auth.login"))

    # Custom error handlers
    from flask import render_template

    @app.errorhandler(403)
    def forbidden(e):
        return render_template("errors/403.html"), 403

    @app.errorhandler(404)
    def not_found(e):
        return render_template("errors/404.html"), 404

    @app.errorhandler(500)
    def server_error(e):
        return render_template("errors/500.html"), 500

    # Create tables in dev
    with app.app_context():
        from app.models import register_models
        register_models()

    return app

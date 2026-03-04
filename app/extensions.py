from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_migrate import Migrate
from flask_wtf.csrf import CSRFProtect
from flask_mail import Mail
from celery import Celery

db = SQLAlchemy()
login_manager = LoginManager()
migrate = Migrate()
csrf = CSRFProtect()
mail = Mail()
celery = Celery(__name__)

login_manager.login_view = "auth.login"
login_manager.login_message_category = "info"

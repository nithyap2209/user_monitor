from functools import wraps
from flask import abort, redirect, url_for
from flask_login import current_user


def permission_required(module, action="view"):
    """Check if current user has specific module.action permission."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for("auth.login"))
            if not current_user.has_permission(module, action):
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def role_required(*role_slugs):
    """Check if current user has one of the specified role slugs."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for("auth.login"))
            if current_user.designation.slug not in role_slugs:
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator

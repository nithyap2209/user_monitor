import secrets
from flask import url_for, current_app
from flask_mail import Message
from app.extensions import mail, db


def generate_verification_token():
    """Generate a secure random token for email verification."""
    return secrets.token_urlsafe(32)


def send_verification_email(user):
    """Send a verification email to the user with a confirmation link."""
    token = generate_verification_token()
    user.verification_token = token
    db.session.commit()

    verify_url = url_for("auth.verify_email", token=token, _external=True)

    msg = Message(
        subject="Verify your User Monitor account",
        recipients=[user.email],
    )

    msg.html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto; padding: 0;">
        <div style="background: linear-gradient(135deg, #4338ca, #6366f1); padding: 32px 24px; text-align: center; border-radius: 12px 12px 0 0;">
            <h1 style="color: #ffffff; font-size: 28px; margin: 0; font-weight: 700;">User Monitor</h1>
            <p style="color: #c7d2fe; font-size: 14px; margin: 6px 0 0;">Social Media Monitoring Platform</p>
        </div>
        <div style="background: #ffffff; padding: 32px 24px; border-left: 1px solid #e5e7eb; border-right: 1px solid #e5e7eb;">
            <h2 style="color: #1f2937; font-size: 20px; margin: 0 0 8px;">Welcome, {user.full_name}!</h2>
            <p style="color: #6b7280; font-size: 14px; line-height: 1.6; margin: 0 0 24px;">
                Thank you for signing up. Please verify your email address to activate your account.
            </p>
            <div style="text-align: center; margin: 24px 0;">
                <a href="{verify_url}"
                   style="display: inline-block; background: #4338ca; color: #ffffff; text-decoration: none;
                          padding: 12px 32px; border-radius: 8px; font-size: 15px; font-weight: 600;">
                    Verify Email Address
                </a>
            </div>
            <p style="color: #9ca3af; font-size: 12px; line-height: 1.5; margin: 24px 0 0;">
                If the button doesn't work, copy and paste this link into your browser:<br>
                <a href="{verify_url}" style="color: #4338ca; word-break: break-all;">{verify_url}</a>
            </p>
        </div>
        <div style="background: #f9fafb; padding: 16px 24px; text-align: center; border-radius: 0 0 12px 12px; border: 1px solid #e5e7eb; border-top: 0;">
            <p style="color: #9ca3af; font-size: 11px; margin: 0;">
                If you didn't create this account, you can safely ignore this email.
            </p>
        </div>
    </div>
    """

    msg.body = f"""Welcome to User Monitor, {user.full_name}!

Please verify your email address by visiting this link:
{verify_url}

If you didn't create this account, you can safely ignore this email.
"""

    try:
        mail.send(msg)
        return True
    except Exception as e:
        current_app.logger.error(f"Failed to send verification email to {user.email}: {e}")
        return False


def send_password_reset_email(user):
    """Send a password reset email to the user."""
    token = generate_verification_token()
    user.password_reset_token = token
    db.session.commit()

    reset_url = url_for("auth.reset_password", token=token, _external=True)

    msg = Message(
        subject="Reset your User Monitor password",
        recipients=[user.email],
    )

    msg.html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto; padding: 0;">
        <div style="background: linear-gradient(135deg, #4338ca, #6366f1); padding: 32px 24px; text-align: center; border-radius: 12px 12px 0 0;">
            <h1 style="color: #ffffff; font-size: 28px; margin: 0; font-weight: 700;">User Monitor</h1>
            <p style="color: #c7d2fe; font-size: 14px; margin: 6px 0 0;">Social Media Monitoring Platform</p>
        </div>
        <div style="background: #ffffff; padding: 32px 24px; border-left: 1px solid #e5e7eb; border-right: 1px solid #e5e7eb;">
            <h2 style="color: #1f2937; font-size: 20px; margin: 0 0 8px;">Password Reset Request</h2>
            <p style="color: #6b7280; font-size: 14px; line-height: 1.6; margin: 0 0 24px;">
                Hi {user.full_name}, we received a request to reset your password. Click the button below to choose a new one.
            </p>
            <div style="text-align: center; margin: 24px 0;">
                <a href="{reset_url}"
                   style="display: inline-block; background: #4338ca; color: #ffffff; text-decoration: none;
                          padding: 12px 32px; border-radius: 8px; font-size: 15px; font-weight: 600;">
                    Reset Password
                </a>
            </div>
            <p style="color: #9ca3af; font-size: 12px; line-height: 1.5; margin: 24px 0 0;">
                If the button doesn't work, copy and paste this link into your browser:<br>
                <a href="{reset_url}" style="color: #4338ca; word-break: break-all;">{reset_url}</a>
            </p>
        </div>
        <div style="background: #f9fafb; padding: 16px 24px; text-align: center; border-radius: 0 0 12px 12px; border: 1px solid #e5e7eb; border-top: 0;">
            <p style="color: #9ca3af; font-size: 11px; margin: 0;">
                If you didn't request a password reset, you can safely ignore this email.
            </p>
        </div>
    </div>
    """

    msg.body = f"""Hi {user.full_name},

We received a request to reset your User Monitor password. Visit this link to set a new password:
{reset_url}

If you didn't request this, you can safely ignore this email.
"""

    try:
        mail.send(msg)
        return True
    except Exception as e:
        current_app.logger.error(f"Failed to send password reset email to {user.email}: {e}")
        return False

import requests as http_requests
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, urlparse, urlunparse
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app
from flask_login import login_required, current_user
from app.extensions import db
from app.models.connected_page import ConnectedPage
from app.models.company_api_key import CompanyAPIKey
from app.models.comment import Comment
from app.models.contact import Contact
from app.models.post import Post
from app.models.post_reaction import PostReaction
from app.utils.decorators import permission_required
from app.utils.constants import PLATFORMS, PLATFORM_LABELS

pages_bp = Blueprint("pages", __name__, url_prefix="/pages")

# ── Server-side OAuth cache ──────────────────────────────
# Flask cookie-based sessions have a ~4 KB limit.  Facebook page data
# (with access tokens) easily exceeds that, causing the session cookie
# to be silently dropped by the browser.  We store the data server-side
# keyed by user ID and retrieve it on the select / confirm pages.
_oauth_cache = {}


def _store_oauth(user_id, pages, platform, user_token, **extra):
    """Save OAuth page data server-side (avoids session size limit)."""
    _oauth_cache[user_id] = {
        "pages": pages,
        "platform": platform,
        "user_token": user_token,
        **extra,
    }


def _pop_oauth(user_id):
    """Retrieve and remove cached OAuth data for a user."""
    return _oauth_cache.pop(user_id, None)

GRAPH_API_BASE = "https://graph.facebook.com/v21.0"
LI_API_BASE = "https://api.linkedin.com/v2"


def _oauth_callback_url():
    """Build the OAuth callback URL.

    - In production: forces HTTPS (Facebook requirement).
    - In local development (localhost / 127.0.0.1): keeps HTTP and
      normalises the host to 'localhost' so Facebook's dev-mode
      exception for http://localhost applies.
    """
    url = url_for("pages.oauth_callback", _external=True)
    parsed = urlparse(url)
    is_local = parsed.hostname in ("localhost", "127.0.0.1", "0.0.0.0")

    if is_local:
        # Facebook dev-mode allows http://localhost (NOT 127.0.0.1)
        url = urlunparse(parsed._replace(scheme="http", netloc=f"localhost:{parsed.port}" if parsed.port else "localhost"))
    else:
        # Production: force HTTPS
        if parsed.scheme == "http":
            url = urlunparse(parsed._replace(scheme="https"))
    return url

# All Facebook permissions — used for both Facebook and Instagram connections
# so that connecting one platform never revokes permissions needed by the other.
FB_ALL_SCOPES = (
    "pages_show_list,pages_read_engagement,pages_manage_posts,"
    "pages_manage_metadata,pages_manage_engagement,"
    "instagram_basic,instagram_manage_comments"
)

# LinkedIn permissions for organization page management (space-separated)
# LinkedIn OAuth scopes (space-separated)
# Open permissions: openid, profile, email, w_member_social
# Organization scopes (require Community Management API approval):
#   r_organization_social, w_organization_social, rw_organization_admin
LI_SCOPES = "openid profile email w_member_social"


@pages_bp.route("/")
@login_required
@permission_required("pages")
def index():
    company_id = current_user.company_id
    connected_pages = ConnectedPage.query.filter_by(company_id=company_id).order_by(ConnectedPage.created_at.desc()).all()
    api_keys = {}
    for platform in PLATFORMS:
        key = CompanyAPIKey.get_for_company(company_id, platform)
        api_keys[platform] = key

    return render_template(
        "pages/index.html",
        connected_pages=connected_pages,
        api_keys=api_keys,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
    )


# ── Facebook OAuth ────────────────────────────────────────

@pages_bp.route("/connect/facebook")
@login_required
@permission_required("pages", "connect")
def connect_facebook():
    """Redirect to Facebook OAuth dialog."""
    creds = CompanyAPIKey.get_for_company(current_user.company_id, "facebook")
    if not creds or not creds.api_key:
        flash("Facebook App ID not configured. Go to Admin > Companies > API Keys first.", "danger")
        return redirect(url_for("pages.index"))

    session["oauth_platform"] = "facebook"
    callback_url = _oauth_callback_url()

    params = urlencode({
        "client_id": creds.api_key,
        "redirect_uri": callback_url,
        "scope": FB_ALL_SCOPES,
        "response_type": "code",
        "auth_type": "rerequest",
        "state": "fb_connect",
    })
    return redirect(f"https://www.facebook.com/v21.0/dialog/oauth?{params}")


# ── Instagram via Facebook OAuth ──────────────────────────

@pages_bp.route("/connect/instagram")
@login_required
@permission_required("pages", "connect")
def connect_instagram():
    """Redirect to Facebook OAuth to discover Instagram Business accounts linked to Pages."""
    creds = CompanyAPIKey.get_for_company(current_user.company_id, "facebook")
    if not creds or not creds.api_key:
        flash("Facebook App ID not configured. Instagram connects via Facebook — configure Facebook API keys first.", "danger")
        return redirect(url_for("pages.index"))

    session["oauth_platform"] = "instagram"
    callback_url = _oauth_callback_url()

    params = urlencode({
        "client_id": creds.api_key,
        "redirect_uri": callback_url,
        "scope": FB_ALL_SCOPES,
        "response_type": "code",
        "auth_type": "rerequest",
        "state": "ig_connect",
    })
    return redirect(f"https://www.facebook.com/v21.0/dialog/oauth?{params}")


# ── LinkedIn OAuth ───────────────────────────────────────

@pages_bp.route("/connect/linkedin")
@login_required
@permission_required("pages", "connect")
def connect_linkedin():
    """Redirect to LinkedIn OAuth 2.0 authorization dialog."""
    creds = CompanyAPIKey.get_for_company(current_user.company_id, "linkedin")
    if not creds or not creds.api_key:
        flash("LinkedIn Client ID not configured. Go to Admin > Companies > API Keys first.", "danger")
        return redirect(url_for("pages.index"))

    session["oauth_platform"] = "linkedin"
    callback_url = _oauth_callback_url()

    params = urlencode({
        "response_type": "code",
        "client_id": creds.api_key,
        "redirect_uri": callback_url,
        "scope": LI_SCOPES,
        "state": "li_connect",
    })
    return redirect(f"https://www.linkedin.com/oauth/v2/authorization?{params}")


# ── YouTube OAuth ────────────────────────────────────────

# YouTube requires OAuth2 for write operations (posting replies).
# The API key (stored in CompanyAPIKey.api_key) handles reads.
# OAuth Client ID is stored in CompanyAPIKey.extra_data["oauth_client_id"]
# OAuth Client Secret is stored in CompanyAPIKey.api_secret
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
YT_OAUTH_SCOPES = "https://www.googleapis.com/auth/youtube.force-ssl"


def _youtube_callback_url():
    """Build the YouTube OAuth callback URL."""
    url = url_for("pages.youtube_oauth_callback", _external=True)
    parsed = urlparse(url)
    is_local = parsed.hostname in ("localhost", "127.0.0.1", "0.0.0.0")
    if is_local:
        url = urlunparse(parsed._replace(
            scheme="http",
            netloc=f"localhost:{parsed.port}" if parsed.port else "localhost",
        ))
    else:
        if parsed.scheme == "http":
            url = urlunparse(parsed._replace(scheme="https"))
    return url


@pages_bp.route("/connect/youtube-oauth")
@login_required
@permission_required("pages", "connect")
def connect_youtube_oauth():
    """Redirect to Google OAuth to authorize YouTube reply access."""
    creds = CompanyAPIKey.get_for_company(current_user.company_id, "youtube")
    if not creds or not creds.api_key:
        flash("YouTube API key not configured. Go to Admin > API Keys first.", "danger")
        return redirect(url_for("pages.index"))

    # OAuth Client ID can be stored in extra_data or in api_secret's companion
    oauth_client_id = None
    if creds.extra_data and isinstance(creds.extra_data, dict):
        oauth_client_id = creds.extra_data.get("oauth_client_id")

    if not oauth_client_id:
        flash(
            "YouTube OAuth Client ID not configured. "
            "Go to Admin > API Keys > YouTube and enter your OAuth Client ID "
            "(from Google Cloud Console > Credentials > OAuth 2.0 Client IDs).",
            "danger",
        )
        return redirect(url_for("pages.index"))

    if not creds.api_secret:
        flash(
            "YouTube OAuth Client Secret not configured. "
            "Go to Admin > API Keys > YouTube and enter your OAuth Client Secret.",
            "danger",
        )
        return redirect(url_for("pages.index"))

    session["oauth_platform"] = "youtube"
    callback_url = _youtube_callback_url()

    params = urlencode({
        "client_id": oauth_client_id,
        "redirect_uri": callback_url,
        "response_type": "code",
        "scope": YT_OAUTH_SCOPES,
        "access_type": "offline",   # Get refresh_token
        "prompt": "consent",        # Force consent to always get refresh_token
        "state": "yt_connect",
    })
    return redirect(f"{GOOGLE_AUTH_URL}?{params}")


@pages_bp.route("/youtube/callback")
@login_required
def youtube_oauth_callback():
    """Handle Google OAuth callback for YouTube."""
    code = request.args.get("code")
    error = request.args.get("error")

    session.pop("oauth_platform", None)

    if error or not code:
        flash(f"YouTube OAuth failed: {error or 'No authorization code received.'}", "danger")
        return redirect(url_for("pages.index"))

    creds = CompanyAPIKey.get_for_company(current_user.company_id, "youtube")
    if not creds:
        flash("YouTube API credentials not found.", "danger")
        return redirect(url_for("pages.index"))

    oauth_client_id = (creds.extra_data or {}).get("oauth_client_id")
    oauth_client_secret = creds.api_secret

    if not oauth_client_id or not oauth_client_secret:
        flash("YouTube OAuth Client ID or Secret missing.", "danger")
        return redirect(url_for("pages.index"))

    callback_url = _youtube_callback_url()

    # Exchange authorization code for access + refresh tokens
    try:
        resp = http_requests.post(GOOGLE_TOKEN_URL, data={
            "code": code,
            "client_id": oauth_client_id,
            "client_secret": oauth_client_secret,
            "redirect_uri": callback_url,
            "grant_type": "authorization_code",
        }, timeout=15)
        token_data = resp.json()
    except Exception as e:
        flash(f"Token exchange failed: {e}", "danger")
        return redirect(url_for("pages.index"))

    if "error" in token_data:
        flash(f"Token error: {token_data.get('error_description', token_data['error'])}", "danger")
        return redirect(url_for("pages.index"))

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = token_data.get("expires_in", 3600)

    if not access_token:
        flash("No access token received from Google.", "danger")
        return redirect(url_for("pages.index"))

    # Save tokens to CompanyAPIKey
    creds.access_token = access_token
    if refresh_token:
        creds.refresh_token = refresh_token
    creds.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    db.session.commit()

    flash("YouTube OAuth connected successfully! You can now reply to YouTube comments.", "success")
    return redirect(url_for("pages.index"))


def _handle_instagram_callback(code):
    """Exchange Facebook auth code for token, then discover Instagram Business accounts."""
    creds = CompanyAPIKey.get_for_company(current_user.company_id, "facebook")
    if not creds or not creds.api_key or not creds.api_secret:
        flash("Facebook App credentials not configured.", "danger")
        return redirect(url_for("pages.index"))

    callback_url = _oauth_callback_url()

    # 1. Exchange code for short-lived Facebook user token
    try:
        resp = http_requests.get(f"{GRAPH_API_BASE}/oauth/access_token", params={
            "client_id": creds.api_key,
            "client_secret": creds.api_secret,
            "redirect_uri": callback_url,
            "code": code,
        }, timeout=15)
        token_data = resp.json()
    except Exception as e:
        flash(f"Token exchange failed: {e}", "danger")
        return redirect(url_for("pages.index"))

    if "error" in token_data:
        flash(f"Token error: {token_data['error'].get('message', 'Unknown')}", "danger")
        return redirect(url_for("pages.index"))

    user_token = token_data.get("access_token")
    if not user_token:
        flash("No access token received.", "danger")
        return redirect(url_for("pages.index"))

    # 2. Exchange for long-lived token
    try:
        resp = http_requests.get(f"{GRAPH_API_BASE}/oauth/access_token", params={
            "grant_type": "fb_exchange_token",
            "client_id": creds.api_key,
            "client_secret": creds.api_secret,
            "fb_exchange_token": user_token,
        }, timeout=15)
        ll_data = resp.json()
        long_lived_token = ll_data.get("access_token", user_token)
    except Exception:
        long_lived_token = user_token

    # 3. Fetch pages with their linked Instagram Business accounts
    try:
        resp = http_requests.get(f"{GRAPH_API_BASE}/me/accounts", params={
            "access_token": long_lived_token,
            "fields": (
                "id,name,access_token,"
                "instagram_business_account{"
                "id,name,username,profile_picture_url,followers_count,media_count"
                "}"
            ),
            "limit": 100,
        }, timeout=15)
        pages_data = resp.json()
    except Exception as e:
        flash(f"Failed to fetch pages: {e}", "danger")
        return redirect(url_for("pages.index"))

    # 4. Collect Instagram Business accounts from the pages
    ig_accounts = []
    for fb_page in pages_data.get("data", []):
        ig = fb_page.get("instagram_business_account")
        if not ig:
            continue
        ig_accounts.append({
            "id": ig["id"],
            "name": ig.get("name") or ig.get("username") or "Instagram Account",
            "username": ig.get("username", ""),
            "profile_picture_url": ig.get("profile_picture_url"),
            "followers_count": ig.get("followers_count", 0),
            "media_count": ig.get("media_count", 0),
            # Use the Page token — it can query the IG account via Graph API
            "access_token": fb_page.get("access_token", long_lived_token),
        })

    if not ig_accounts:
        flash(
            "No Instagram Business accounts found. Make sure your Instagram "
            "account is converted to a Business or Creator account and linked "
            "to one of your Facebook Pages.",
            "warning",
        )
        return redirect(url_for("pages.index"))

    # Single account → auto-connect
    if len(ig_accounts) == 1:
        acct = ig_accounts[0]
        existing = ConnectedPage.query.filter_by(
            company_id=current_user.company_id,
            platform="instagram",
            page_id=acct["id"],
        ).first()
        if existing:
            existing.page_name = acct["name"]
            existing.access_token = acct["access_token"]
            existing.page_avatar = acct.get("profile_picture_url")
            existing.followers_count = acct.get("followers_count", 0)
            existing.status = "connected"
            if acct.get("username"):
                existing.page_url = f"https://www.instagram.com/{acct['username']}/"
            db.session.commit()
            flash(f"{existing.page_name} is already connected and has been updated.", "info")
            return redirect(url_for("pages.index"))

        page = ConnectedPage(
            company_id=current_user.company_id,
            platform="instagram",
            page_id=acct["id"],
            page_name=acct["name"],
            access_token=acct["access_token"],
            page_avatar=acct.get("profile_picture_url"),
            page_url=f"https://www.instagram.com/{acct['username']}/" if acct.get("username") else None,
            followers_count=acct.get("followers_count", 0),
            status="connected",
        )
        db.session.add(page)
        db.session.commit()
        flash(f"{page.page_name} connected successfully!", "success")
        return redirect(url_for("pages.index"))

    # Multiple accounts → let user choose
    _store_oauth(current_user.id, ig_accounts, "instagram", long_lived_token)
    return redirect(url_for("pages.select_page"))


def _handle_linkedin_callback(code):
    """Exchange LinkedIn auth code for token and fetch organizations."""
    creds = CompanyAPIKey.get_for_company(current_user.company_id, "linkedin")
    if not creds or not creds.api_key or not creds.api_secret:
        flash("LinkedIn Client ID or Client Secret not configured.", "danger")
        return redirect(url_for("pages.index"))

    callback_url = _oauth_callback_url()

    # 1. Exchange authorization code for access token (POST, not GET)
    try:
        resp = http_requests.post(
            "https://www.linkedin.com/oauth/v2/accessToken",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": callback_url,
                "client_id": creds.api_key,
                "client_secret": creds.api_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        token_data = resp.json()
    except Exception as e:
        flash(f"LinkedIn token exchange failed: {e}", "danger")
        return redirect(url_for("pages.index"))

    access_token = token_data.get("access_token")
    if not access_token:
        error_desc = token_data.get("error_description", "No access token received.")
        flash(f"LinkedIn token error: {error_desc}", "danger")
        return redirect(url_for("pages.index"))

    expires_in = token_data.get("expires_in", 5184000)  # default 60 days
    refresh_token = token_data.get("refresh_token")

    # 2. Fetch organizations the user administers
    li_headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
    }

    try:
        resp = http_requests.get(
            f"{LI_API_BASE}/organizationalEntityAcls",
            headers=li_headers,
            params={
                "q": "roleAssignee",
                "role": "ADMINISTRATOR",
                "projection": "(elements*(organizationalTarget))",
            },
            timeout=15,
        )
        acls_data = resp.json()
    except Exception as e:
        flash(f"Failed to fetch LinkedIn organizations: {e}", "danger")
        return redirect(url_for("pages.index"))

    elements = acls_data.get("elements", [])
    if not elements:
        flash(
            "No LinkedIn organization pages found. "
            "Make sure you are an admin of at least one LinkedIn Company Page.",
            "warning",
        )
        return redirect(url_for("pages.index"))

    # 3. For each organization, fetch details
    orgs = []
    for el in elements:
        org_urn = el.get("organizationalTarget", "")
        org_id = org_urn.split(":")[-1] if org_urn else None
        if not org_id:
            continue

        try:
            org_resp = http_requests.get(
                f"{LI_API_BASE}/organizations/{org_id}",
                headers=li_headers,
                params={"projection": "(id,localizedName,vanityName,logoV2,followersCount)"},
                timeout=10,
            )
            org_data = org_resp.json()
        except Exception:
            org_data = {"id": int(org_id)}

        # Extract logo URL (may be a URN which we skip)
        logo_url = None
        logo_v2 = org_data.get("logoV2", {})
        if logo_v2:
            original = logo_v2.get("original", logo_v2.get("cropped", ""))
            if isinstance(original, str) and not original.startswith("urn:"):
                logo_url = original

        orgs.append({
            "id": str(org_data.get("id", org_id)),
            "name": org_data.get("localizedName", f"Organization {org_id}"),
            "vanity_name": org_data.get("vanityName", ""),
            "followers_count": org_data.get("followersCount", 0),
            "logo_url": logo_url,
        })

    if not orgs:
        flash("Could not fetch organization details from LinkedIn.", "warning")
        return redirect(url_for("pages.index"))

    # 4. Store server-side for the selection page
    _store_oauth(
        current_user.id, orgs, "linkedin", access_token,
        refresh_token=refresh_token,
        token_expires_at=(datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat(),
    )

    return redirect(url_for("pages.select_page"))


# ── OAuth Callback (shared for FB, IG & LinkedIn) ────────

@pages_bp.route("/callback")
@login_required
def oauth_callback():
    """Handle OAuth callback from Facebook/Instagram/LinkedIn."""
    code = request.args.get("code")
    error = request.args.get("error")
    error_description = request.args.get("error_description") or request.args.get("error", "Authorization denied.")
    state = request.args.get("state", "")

    # Determine platform from state param (reliable) or session (fallback)
    STATE_PLATFORM_MAP = {
        "ig_connect": "instagram",
        "fb_connect": "facebook",
        "li_connect": "linkedin",
    }
    platform = STATE_PLATFORM_MAP.get(state) or session.pop("oauth_platform", "facebook")
    session.pop("oauth_platform", None)  # clean up session regardless

    if error or not code:
        flash(f"OAuth failed: {error_description}", "danger")
        return redirect(url_for("pages.index"))

    # LinkedIn has its own token exchange flow
    if platform == "linkedin":
        return _handle_linkedin_callback(code)

    # Instagram Login has its own token exchange flow (IGAAS tokens)
    if platform == "instagram":
        return _handle_instagram_callback(code)

    # ── Facebook flow ──
    creds = CompanyAPIKey.get_for_company(current_user.company_id, "facebook")
    if not creds or not creds.api_key or not creds.api_secret:
        flash("Facebook App ID or App Secret not configured.", "danger")
        return redirect(url_for("pages.index"))

    callback_url = _oauth_callback_url()

    # 1. Exchange code for short-lived user token
    try:
        resp = http_requests.get(f"{GRAPH_API_BASE}/oauth/access_token", params={
            "client_id": creds.api_key,
            "client_secret": creds.api_secret,
            "redirect_uri": callback_url,
            "code": code,
        }, timeout=15)
        token_data = resp.json()
    except Exception as e:
        flash(f"Token exchange failed: {e}", "danger")
        return redirect(url_for("pages.index"))

    if "error" in token_data:
        flash(f"Token error: {token_data['error'].get('message', 'Unknown')}", "danger")
        return redirect(url_for("pages.index"))

    user_token = token_data.get("access_token")
    if not user_token:
        flash("No access token received.", "danger")
        return redirect(url_for("pages.index"))

    # 2. Exchange for long-lived token
    try:
        resp = http_requests.get(f"{GRAPH_API_BASE}/oauth/access_token", params={
            "grant_type": "fb_exchange_token",
            "client_id": creds.api_key,
            "client_secret": creds.api_secret,
            "fb_exchange_token": user_token,
        }, timeout=15)
        ll_data = resp.json()
        long_lived_token = ll_data.get("access_token", user_token)
    except Exception:
        long_lived_token = user_token

    # 3. Check granted permissions to help debug issues
    try:
        perm_resp = http_requests.get(f"{GRAPH_API_BASE}/me/permissions", params={
            "access_token": long_lived_token,
        }, timeout=10)
        perm_data = perm_resp.json()
        granted = [p["permission"] for p in perm_data.get("data", []) if p.get("status") == "granted"]
        declined = [p["permission"] for p in perm_data.get("data", []) if p.get("status") == "declined"]
        current_app.logger.info(f"[FB OAuth] Granted permissions: {granted}")
        if declined:
            current_app.logger.warning(f"[FB OAuth] Declined permissions: {declined}")
            flash(f"Some permissions were declined: {', '.join(declined)}. Please reconnect and grant all permissions.", "warning")
    except Exception:
        pass

    # 4. Fetch user's pages
    try:
        resp = http_requests.get(f"{GRAPH_API_BASE}/me/accounts", params={
            "access_token": long_lived_token,
            "fields": "id,name,access_token,picture,fan_count,category",
            "limit": 100,
        }, timeout=15)
        pages_data = resp.json()
        current_app.logger.info(f"[FB OAuth] /me/accounts response: {pages_data}")
    except Exception as e:
        flash(f"Failed to fetch pages: {e}", "danger")
        return redirect(url_for("pages.index"))

    if "error" in pages_data:
        error_msg = pages_data["error"].get("message", "Unknown error")
        flash(f"Facebook API error: {error_msg}", "danger")
        return redirect(url_for("pages.index"))

    fb_pages = pages_data.get("data", [])

    if not fb_pages:
        flash(
            "No Facebook pages found. This can happen if: "
            "(1) the account doesn't manage any Facebook Page, "
            "(2) 'pages_show_list' permission was declined, or "
            "(3) the app is in Development mode and the user is not added as a tester/developer.",
            "warning",
        )
        return redirect(url_for("pages.index"))

    _store_oauth(current_user.id, fb_pages, platform, long_lived_token)

    return redirect(url_for("pages.select_page"))


@pages_bp.route("/select")
@login_required
@permission_required("pages", "connect")
def select_page():
    """Show available pages for the user to select."""
    cached = _oauth_cache.get(current_user.id)
    if not cached or not cached.get("pages"):
        flash("No pages available. Try connecting again.", "warning")
        return redirect(url_for("pages.index"))

    pages_list = cached["pages"]
    platform = cached.get("platform", "facebook")

    return render_template(
        "pages/select.html",
        pages_list=pages_list,
        platform=platform,
        platform_label=PLATFORM_LABELS.get(platform, platform),
    )


@pages_bp.route("/select/confirm", methods=["POST"])
@login_required
@permission_required("pages", "connect")
def confirm_page():
    """Save the selected page as a ConnectedPage."""
    selected_index = request.form.get("page_index", type=int)
    cached = _pop_oauth(current_user.id) or {}
    pages_list = cached.get("pages", [])
    platform = cached.get("platform", "facebook")
    user_token = cached.get("user_token", "")

    if selected_index is None or selected_index >= len(pages_list):
        flash("Invalid selection.", "danger")
        return redirect(url_for("pages.index"))

    selected = pages_list[selected_index]

    if platform == "linkedin":
        # Parse token expiry from cache
        expires_str = cached.get("token_expires_at")
        token_expires = None
        if expires_str:
            try:
                token_expires = datetime.fromisoformat(expires_str)
            except (ValueError, TypeError):
                pass
        refresh_token = cached.get("refresh_token")

        existing = ConnectedPage.query.filter_by(
            company_id=current_user.company_id,
            platform="linkedin",
            page_id=selected.get("id"),
        ).first()
        if existing:
            existing.page_name = selected.get("name", "LinkedIn Organization")
            existing.access_token = user_token
            existing.refresh_token = refresh_token
            existing.token_expires_at = token_expires
            existing.page_avatar = selected.get("logo_url")
            existing.page_url = (
                f"https://www.linkedin.com/company/{selected.get('vanity_name')}"
                if selected.get("vanity_name") else existing.page_url
            )
            existing.followers_count = selected.get("followers_count", 0)
            existing.status = "connected"
            db.session.commit()
            flash(f"{existing.page_name} is already connected and has been updated.", "info")
            return redirect(url_for("pages.index"))

        page = ConnectedPage(
            company_id=current_user.company_id,
            platform="linkedin",
            page_id=selected.get("id"),
            page_name=selected.get("name", "LinkedIn Organization"),
            access_token=user_token,
            refresh_token=refresh_token,
            token_expires_at=token_expires,
            page_avatar=selected.get("logo_url"),
            page_url=(
                f"https://www.linkedin.com/company/{selected.get('vanity_name')}"
                if selected.get("vanity_name") else None
            ),
            followers_count=selected.get("followers_count", 0),
            status="connected",
        )
    elif platform == "instagram":
        existing = ConnectedPage.query.filter_by(
            company_id=current_user.company_id,
            platform="instagram",
            page_id=selected.get("id"),
        ).first()
        if existing:
            existing.page_name = selected.get("name", "Instagram Account")
            existing.access_token = selected.get("access_token", user_token)
            existing.page_avatar = selected.get("profile_picture_url")
            existing.followers_count = selected.get("followers_count", 0)
            existing.status = "connected"
            if selected.get("username"):
                existing.page_url = f"https://www.instagram.com/{selected['username']}/"
            db.session.commit()
            flash(f"{existing.page_name} is already connected and has been updated.", "info")
            return redirect(url_for("pages.index"))

        page = ConnectedPage(
            company_id=current_user.company_id,
            platform="instagram",
            page_id=selected.get("id"),
            page_name=selected.get("name", "Instagram Account"),
            access_token=selected.get("access_token", user_token),
            page_avatar=selected.get("profile_picture_url"),
            page_url=f"https://www.instagram.com/{selected['username']}/" if selected.get("username") else None,
            followers_count=selected.get("followers_count", 0),
            status="connected",
        )
    else:
        picture_url = None
        pic = selected.get("picture", {})
        if isinstance(pic, dict):
            picture_url = pic.get("data", {}).get("url")

        existing = ConnectedPage.query.filter_by(
            company_id=current_user.company_id,
            platform="facebook",
            page_id=selected.get("id"),
        ).first()
        if existing:
            existing.page_name = selected.get("name", "Facebook Page")
            existing.access_token = selected.get("access_token", user_token)
            existing.page_avatar = picture_url or existing.page_avatar
            existing.followers_count = selected.get("fan_count", 0)
            existing.status = "connected"
            db.session.commit()
            flash(f"{existing.page_name} is already connected and has been updated.", "info")
            return redirect(url_for("pages.index"))

        page = ConnectedPage(
            company_id=current_user.company_id,
            platform="facebook",
            page_id=selected.get("id"),
            page_name=selected.get("name", "Facebook Page"),
            access_token=selected.get("access_token", user_token),
            page_avatar=picture_url,
            followers_count=selected.get("fan_count", 0),
            status="connected",
        )

    db.session.add(page)
    db.session.commit()
    flash(f"{page.page_name} connected successfully!", "success")
    return redirect(url_for("pages.index"))


# ── Manual Connect (for other platforms) ──────────────────

@pages_bp.route("/connect", methods=["POST"])
@login_required
@permission_required("pages", "connect")
def connect():
    """Manual connect for platforms without OAuth (YouTube, Twitter, Google)."""
    platform = request.form.get("platform")
    page_id_val = request.form.get("page_id", "").strip()
    page_name = request.form.get("page_name", "").strip()

    if not platform or not page_id_val:
        flash("Platform and Page ID are required.", "danger")
        return redirect(url_for("pages.index"))

    # YouTube: validate API key and resolve channel info
    if platform == "youtube":
        creds = CompanyAPIKey.get_for_company(current_user.company_id, "youtube")
        if not creds or not creds.api_key:
            flash("YouTube API key not configured. Go to Admin > API Keys and add your YouTube Data API key first.", "danger")
            return redirect(url_for("pages.index"))

        # Try to resolve channel info via the API
        try:
            params = {
                "key": creds.api_key,
                "part": "snippet,statistics",
            }
            # Support channel ID, handle/username, or custom URL
            if page_id_val.startswith("UC") and len(page_id_val) == 24:
                params["id"] = page_id_val
            elif page_id_val.startswith("@"):
                params["forHandle"] = page_id_val
            else:
                # Could be a channel ID or handle without @
                params["id"] = page_id_val

            resp = http_requests.get(
                "https://www.googleapis.com/youtube/v3/channels",
                params=params,
                timeout=15,
            )
            data = resp.json()

            if "error" in data:
                error_msg = data["error"].get("message", "Unknown error")
                flash(f"YouTube API error: {error_msg}", "danger")
                return redirect(url_for("pages.index"))

            items = data.get("items", [])
            if not items:
                # Retry with forHandle if the raw ID didn't work
                if not page_id_val.startswith("@") and not page_id_val.startswith("UC"):
                    params.pop("id", None)
                    params["forHandle"] = f"@{page_id_val}"
                    resp = http_requests.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params=params,
                        timeout=15,
                    )
                    data = resp.json()
                    items = data.get("items", [])

            if not items:
                flash("No YouTube channel found. Check the Channel ID or handle and try again.", "warning")
                return redirect(url_for("pages.index"))

            channel = items[0]
            snippet = channel.get("snippet", {})
            statistics = channel.get("statistics", {})
            resolved_id = channel.get("id", page_id_val)
            resolved_name = page_name or snippet.get("title", resolved_id)
            avatar = (snippet.get("thumbnails") or {}).get("default", {}).get("url")
            followers = int(statistics.get("subscriberCount", 0))

            existing = ConnectedPage.query.filter_by(
                company_id=current_user.company_id,
                platform="youtube",
                page_id=resolved_id,
            ).first()
            if existing:
                existing.page_name = resolved_name
                existing.page_avatar = avatar
                existing.page_url = f"https://www.youtube.com/channel/{resolved_id}"
                existing.followers_count = followers
                existing.status = "connected"
                db.session.commit()
                flash(f"YouTube channel \"{resolved_name}\" is already connected and has been updated.", "info")
                return redirect(url_for("pages.index"))

            page = ConnectedPage(
                company_id=current_user.company_id,
                platform="youtube",
                page_id=resolved_id,
                page_name=resolved_name,
                page_avatar=avatar,
                page_url=f"https://www.youtube.com/channel/{resolved_id}",
                followers_count=followers,
                status="connected",
            )
            db.session.add(page)
            db.session.commit()
            flash(f"YouTube channel \"{resolved_name}\" connected successfully!", "success")
            return redirect(url_for("pages.index"))

        except http_requests.RequestException as e:
            flash(f"Failed to verify YouTube channel: {e}", "danger")
            return redirect(url_for("pages.index"))

    # Other platforms: simple manual connect
    page = ConnectedPage(
        company_id=current_user.company_id,
        platform=platform,
        page_id=page_id_val,
        page_name=page_name or page_id_val,
        status="connected",
    )
    db.session.add(page)
    db.session.commit()
    flash(f"{page_name or page_id_val} connected!", "success")
    return redirect(url_for("pages.index"))


@pages_bp.route("/clear-data/<int:page_id>", methods=["POST"])
@login_required
@permission_required("pages", "disconnect")
def clear_data(page_id):
    """Delete all posts, comments, and contacts for a page but keep the connection."""
    import time
    from sqlalchemy.exc import OperationalError

    page = ConnectedPage.query.get_or_404(page_id)
    if page.company_id != current_user.company_id:
        flash("Unauthorized.", "danger")
        return redirect(url_for("pages.index"))

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Get all post IDs for this page
            post_ids = [p.id for p in Post.query.filter_by(connected_page_id=page.id).with_entities(Post.id).all()]

            if post_ids:
                # Delete in batches to reduce lock duration
                batch_size = 50
                for i in range(0, len(post_ids), batch_size):
                    batch = post_ids[i:i + batch_size]
                    Contact.query.filter(Contact.source_post_id.in_(batch)).delete(synchronize_session=False)
                    PostReaction.query.filter(PostReaction.post_id.in_(batch)).delete(synchronize_session=False)
                    Comment.query.filter(Comment.post_id.in_(batch)).delete(synchronize_session=False)

            # Delete all posts
            Post.query.filter_by(connected_page_id=page.id).delete(synchronize_session=False)

            # Reset sync timestamp
            page.last_synced_at = None
            db.session.commit()

            flash(f"All data for {page.page_name} has been cleared.", "success")
            return redirect(url_for("pages.index"))
        except OperationalError:
            db.session.rollback()
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                flash("Database is busy (a sync may be running). Please try again in a moment.", "warning")
                return redirect(url_for("pages.index"))


@pages_bp.route("/disconnect/<int:page_id>", methods=["POST"])
@login_required
@permission_required("pages", "disconnect")
def disconnect(page_id):
    page = ConnectedPage.query.get_or_404(page_id)
    if page.company_id != current_user.company_id:
        flash("Unauthorized.", "danger")
        return redirect(url_for("pages.index"))

    page.status = "disconnected"
    db.session.commit()
    flash(f"{page.page_name} disconnected.", "info")
    return redirect(url_for("pages.index"))


@pages_bp.route("/remove/<int:page_id>", methods=["POST"])
@login_required
@permission_required("pages", "disconnect")
def remove(page_id):
    page = ConnectedPage.query.get_or_404(page_id)
    if page.company_id != current_user.company_id:
        flash("Unauthorized.", "danger")
        return redirect(url_for("pages.index"))

    name = page.page_name

    # Delete contacts linked to this page's posts before cascade removes the posts
    post_ids = [p.id for p in Post.query.filter_by(connected_page_id=page.id).with_entities(Post.id).all()]
    if post_ids:
        Contact.query.filter(Contact.source_post_id.in_(post_ids)).delete(synchronize_session=False)
        PostReaction.query.filter(PostReaction.post_id.in_(post_ids)).delete(synchronize_session=False)

    db.session.delete(page)
    db.session.commit()
    flash(f"{name} and all associated data removed.", "success")
    return redirect(url_for("pages.index"))

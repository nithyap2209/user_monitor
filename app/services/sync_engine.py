"""Unified sync orchestrator.

Routes sync requests to the correct platform service based on the
ConnectedPage's platform.  Uses the page's own access_token (from OAuth)
or falls back to company-level CompanyAPIKey credentials.
"""

import re
import requests as http_requests
from datetime import datetime, timezone

from app.extensions import db
from app.models.connected_page import ConnectedPage
from app.models.post import Post
from app.models.comment import Comment
from app.models.contact import Contact
from app.models.post_reaction import PostReaction
from app.models.company_api_key import CompanyAPIKey
from app.services.facebook_service import FacebookService
from app.services.instagram_service import InstagramService
from app.services.youtube_service import YouTubeService
from app.services.linkedin_service import LinkedInService
from app.services.twitter_service import TwitterService
from app.services.google_reviews import GoogleReviewsService

import time
from sqlalchemy.exc import OperationalError


def _safe_commit(max_retries=3, delay=1):
    """Commit with retries to handle SQLite 'database is locked' errors."""
    for attempt in range(max_retries):
        try:
            db.session.commit()
            return
        except OperationalError:
            db.session.rollback()
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                raise


SERVICE_MAP = {
    "facebook": FacebookService,
    "instagram": InstagramService,
    "youtube": YouTubeService,
    "linkedin": LinkedInService,
    "twitter": TwitterService,
    "google_reviews": GoogleReviewsService,
}

GRAPH_API_BASE = "https://graph.facebook.com/v21.0"

# Simple patterns for contact extraction
PHONE_RE = re.compile(
    r"(?:\+?\d{1,3}[-.\s]?)?"   # optional country code (+91, +1, etc.)
    r"\(?\d{2,5}\)?"             # area code / first group (2-5 digits)
    r"[-.\s]?"
    r"\d{3,5}"                   # middle group
    r"[-.\s]?"
    r"\d{3,5}"                   # last group  (total >= 7 digits)
)
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
# Pattern to reject date-like strings that slip through
DATE_RE = re.compile(r"^\d{1,4}[-/]\d{1,2}[-/]\d{2,4}(?:\s|$)")


def get_service(company_id, platform):
    """Get the appropriate service instance for a company + platform."""
    service_cls = SERVICE_MAP.get(platform)
    if not service_cls:
        return None
    return service_cls(company_id)


def _paginate_graph_api(url, params, timeout=60, retries=3):
    """Fetch all pages from a Facebook/Instagram Graph API endpoint.

    The Graph API returns paginated results with a 'paging.next' URL.
    This helper follows all pages and returns the combined 'data' list.
    Retries transient network errors with backoff.
    """
    all_data = []

    # First request with retries
    result = None
    for attempt in range(retries):
        try:
            resp = http_requests.get(url, params=params, timeout=timeout)
            result = resp.json()
            break
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
            else:
                return all_data, str(e)

    if "error" in result:
        return all_data, result["error"].get("message", "Unknown error")

    all_data.extend(result.get("data", []))

    # Follow all pagination cursors until exhausted
    while True:
        next_url = (result.get("paging") or {}).get("next")
        if not next_url:
            break
        try:
            resp = http_requests.get(next_url, timeout=timeout)
            result = resp.json()
        except Exception:
            break
        if "error" in result:
            break
        page_data = result.get("data", [])
        if not page_data:
            break
        all_data.extend(page_data)

    return all_data, None


# ── Facebook sync ───────────────────────────────────────────

def _sync_facebook(page):
    """Fetch posts and comments from a Facebook page using its OAuth token."""
    from app.services.ai_service import _heuristic_sentiment

    token = page.access_token
    if not token:
        return {"error": "No access token for this page."}

    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}

    # Pre-load existing post IDs for this page (avoids per-post DB query)
    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    # Fetch all posts (paginated, follows all pages)
    posts_data, err = _paginate_graph_api(
        f"{GRAPH_API_BASE}/{page.page_id}/posts",
        params={
            "access_token": token,
            "fields": "id,message,created_time,full_picture,shares,permalink_url,likes.summary(true),comments.summary(true)",
            "limit": 100,
        },
    )
    if err and not posts_data:
        return {"error": f"Failed to fetch posts: {err}"}

    for p in posts_data:
        platform_post_id = p.get("id", "")

        # Parse engagement counts from Graph API summary objects
        api_likes = (p.get("likes") or {}).get("summary", {}).get("total_count", 0)
        api_comments = (p.get("comments") or {}).get("summary", {}).get("total_count", 0)
        api_shares = (p.get("shares") or {}).get("count", 0)

        existing = existing_posts.get(platform_post_id)

        if existing:
            post = existing
            # Incremental: skip if nothing changed AND all comments already synced
            counts_changed = (
                post.likes_count != api_likes
                or post.comments_count != api_comments
                or post.shares_count != api_shares
            )
            if not counts_changed:
                # Still check: maybe a previous sync stored the post but
                # didn't finish fetching all comments (e.g. network error)
                db_comment_count = Comment.query.filter_by(post_id=post.id).count()
                if db_comment_count >= api_comments:
                    continue  # truly nothing new
                # Fall through so missing comments are fetched

            likes_changed = post.likes_count != api_likes
            comments_changed = post.comments_count != api_comments or (
                not counts_changed and Comment.query.filter_by(post_id=post.id).count() < api_comments
            )

            post.caption = p.get("message") or post.caption
            post.media_url = p.get("full_picture") or post.media_url
            post.permalink = p.get("permalink_url") or post.permalink
            post.likes_count = api_likes
            post.comments_count = api_comments
            post.shares_count = api_shares
            post.synced_at = datetime.now(timezone.utc)
        else:
            likes_changed = api_likes > 0
            comments_changed = api_comments > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="facebook",
                platform_post_id=platform_post_id,
                caption=p.get("message", ""),
                media_url=p.get("full_picture"),
                media_type="image" if p.get("full_picture") else "text",
                permalink=p.get("permalink_url"),
                likes_count=api_likes,
                comments_count=api_comments,
                shares_count=api_shares,
                posted_at=_parse_fb_time(p.get("created_time")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()  # ensure post.id is set
        stats["posts_synced"] += 1

        # Sync individual reactions (user IDs and names) when likes changed
        if likes_changed and api_likes > 0:
            _sync_facebook_reactions(post, page, token)

        # Skip comment fetch if comments count unchanged or zero
        if not comments_changed or api_comments == 0:
            continue

        # Pre-load existing comment IDs for this post (one query instead of N)
        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id)
            .all()
        )

        # Fetch all comments for this post
        # filter=stream includes page-owner comments and nested replies
        comments_data, _ = _paginate_graph_api(
            f"{GRAPH_API_BASE}/{platform_post_id}/comments",
            params={
                "access_token": token,
                "fields": "id,from,message,created_time,like_count",
                "filter": "stream",
                "limit": 100,
            },
        )

        pending_contacts = []
        for c in comments_data:
            c_id = c.get("id", "")
            if c_id in existing_comment_ids:
                continue  # already synced — no DB query needed

            author = c.get("from", {})
            author_id = author.get("id", "")
            author_name = author.get("name") or (f"User {author_id}" if author_id else "Unknown")
            comment_text = c.get("message", "")
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)

            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="facebook",
                platform_comment_id=c_id,
                platform_author_id=author_id or None,
                author_name=author_name,
                comment_text=comment_text,
                likes_count=c.get("like_count", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=_parse_fb_time(c.get("created_time")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1

            if has_contact:
                pending_contacts.append(comment)

        # Flush once per post to get comment IDs, then extract contacts
        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

    # Update page last synced
    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    return stats


def sync_facebook_stream(page):
    """Generator: streams per-post progress events for Facebook sync (SSE-friendly)."""
    from app.services.ai_service import _heuristic_sentiment

    token = page.access_token
    if not token:
        yield {"type": "error", "error": "No access token for this page."}
        return

    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}

    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    posts_data, err = _paginate_graph_api(
        f"{GRAPH_API_BASE}/{page.page_id}/posts",
        params={
            "access_token": token,
            "fields": "id,message,created_time,full_picture,shares,permalink_url,likes.summary(true),comments.summary(true)",
            "limit": 100,
        },
    )
    if err and not posts_data:
        yield {"type": "error", "error": f"Failed to fetch posts: {err}"}
        return

    total_posts = len(posts_data)
    yield {"type": "start", "total": total_posts}

    for idx, p in enumerate(posts_data):
        platform_post_id = p.get("id", "")
        api_likes = (p.get("likes") or {}).get("summary", {}).get("total_count", 0)
        api_comments = (p.get("comments") or {}).get("summary", {}).get("total_count", 0)
        api_shares = (p.get("shares") or {}).get("count", 0)
        caption = (p.get("message") or "")[:60] or "(No message)"
        thumbnail = p.get("full_picture")
        permalink = p.get("permalink_url")
        post_comments_synced = 0

        existing = existing_posts.get(platform_post_id)

        if existing:
            post = existing
            counts_changed = (
                post.likes_count != api_likes
                or post.comments_count != api_comments
                or post.shares_count != api_shares
            )
            if not counts_changed:
                db_comment_count = Comment.query.filter_by(post_id=post.id).count()
                if db_comment_count >= api_comments:
                    yield {"type": "post", "index": idx + 1, "total": total_posts,
                           "title": caption, "thumbnail": thumbnail, "permalink": permalink,
                           "likes": api_likes, "comments_synced": 0, "skipped": True}
                    continue

            likes_changed = post.likes_count != api_likes
            comments_changed = post.comments_count != api_comments or (
                not counts_changed and Comment.query.filter_by(post_id=post.id).count() < api_comments
            )
            post.caption = p.get("message") or post.caption
            post.media_url = p.get("full_picture") or post.media_url
            post.permalink = p.get("permalink_url") or post.permalink
            post.likes_count = api_likes
            post.comments_count = api_comments
            post.shares_count = api_shares
            post.synced_at = datetime.now(timezone.utc)
        else:
            likes_changed = api_likes > 0
            comments_changed = api_comments > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="facebook",
                platform_post_id=platform_post_id,
                caption=p.get("message", ""),
                media_url=p.get("full_picture"),
                media_type="image" if p.get("full_picture") else "text",
                permalink=p.get("permalink_url"),
                likes_count=api_likes,
                comments_count=api_comments,
                shares_count=api_shares,
                posted_at=_parse_fb_time(p.get("created_time")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        if likes_changed and api_likes > 0:
            _sync_facebook_reactions(post, page, token)

        if not comments_changed or api_comments == 0:
            yield {"type": "post", "index": idx + 1, "total": total_posts,
                   "title": caption, "thumbnail": thumbnail, "permalink": permalink,
                   "likes": api_likes, "comments_synced": 0}
            continue

        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id)
            .all()
        )

        post_comments_data, _ = _paginate_graph_api(
            f"{GRAPH_API_BASE}/{platform_post_id}/comments",
            params={
                "access_token": token,
                "fields": "id,from,message,created_time,like_count",
                "filter": "stream",
                "limit": 100,
            },
        )

        pending_contacts = []
        for c in post_comments_data:
            c_id = c.get("id", "")
            if c_id in existing_comment_ids:
                continue
            author = c.get("from", {})
            author_id = author.get("id", "")
            author_name = author.get("name") or (f"User {author_id}" if author_id else "Unknown")
            comment_text = c.get("message", "")
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)
            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="facebook",
                platform_comment_id=c_id,
                platform_author_id=author_id or None,
                author_name=author_name,
                comment_text=comment_text,
                likes_count=c.get("like_count", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=_parse_fb_time(c.get("created_time")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1
            post_comments_synced += 1
            if has_contact:
                pending_contacts.append(comment)

        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

        yield {"type": "post", "index": idx + 1, "total": total_posts,
               "title": caption, "thumbnail": thumbnail, "permalink": permalink,
               "likes": api_likes, "comments_synced": post_comments_synced}

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    yield {"type": "done", **stats}


def _sync_facebook_reactions(post, page, token):
    """Fetch individual reactions for a Facebook post and store them."""
    reactions_data, err = _paginate_graph_api(
        f"{GRAPH_API_BASE}/{post.platform_post_id}/reactions",
        params={
            "access_token": token,
            "fields": "id,name,type",
            "limit": 100,
        },
    )
    if err or not reactions_data:
        return

    # Pre-load existing reaction user IDs for dedup
    existing_user_ids = set(
        r.platform_user_id
        for r in PostReaction.query.filter_by(post_id=post.id)
        .with_entities(PostReaction.platform_user_id)
        .all()
    )

    for r in reactions_data:
        user_id = r.get("id", "")
        if user_id in existing_user_ids:
            continue

        reaction = PostReaction(
            post_id=post.id,
            company_id=page.company_id,
            platform="facebook",
            platform_user_id=user_id,
            user_name=r.get("name", "Unknown"),
            reaction_type=r.get("type", "LIKE"),
            synced_at=datetime.now(timezone.utc),
        )
        db.session.add(reaction)

    db.session.flush()


# ── Instagram sync ──────────────────────────────────────────

def _ig_api_base(token):
    """Pick the right Graph API host based on Instagram token type.

    Instagram Login tokens (IGAAS...) use graph.instagram.com.
    Facebook Page tokens (EAA...) use graph.facebook.com.
    """
    if token and token.startswith("IGAAS"):
        return "https://graph.instagram.com/v21.0"
    return GRAPH_API_BASE


def _sync_instagram(page):
    """Fetch media and comments from an Instagram business account."""
    from app.services.ai_service import _heuristic_sentiment

    token = page.access_token
    if not token:
        return {"error": "No access token for this page."}

    api_base = _ig_api_base(token)
    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}
    media_type_map = {"IMAGE": "image", "VIDEO": "video", "CAROUSEL_ALBUM": "carousel"}

    # Pre-load existing post IDs for this page
    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    # Fetch all media (paginated, follows all pages)
    media_data, err = _paginate_graph_api(
        f"{api_base}/{page.page_id}/media",
        params={
            "access_token": token,
            "fields": "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count",
            "limit": 100,
        },
    )
    if err and not media_data:
        return {"error": f"Failed to fetch media: {err}"}

    for m in media_data:
        platform_post_id = m.get("id", "")
        api_comments_count = m.get("comments_count", 0)
        api_likes = m.get("like_count", 0)

        existing = existing_posts.get(platform_post_id)

        if existing:
            post = existing
            # Incremental: skip if nothing changed AND all comments already synced
            counts_changed = (
                post.likes_count != api_likes
                or post.comments_count != api_comments_count
            )
            if not counts_changed:
                db_comment_count = Comment.query.filter_by(post_id=post.id).count()
                if db_comment_count >= api_comments_count:
                    continue  # truly nothing new

            comments_changed = post.comments_count != api_comments_count or (
                not counts_changed and Comment.query.filter_by(post_id=post.id).count() < api_comments_count
            )

            post.caption = m.get("caption") or post.caption
            post.media_url = m.get("media_url") or post.media_url
            post.permalink = m.get("permalink") or post.permalink
            post.likes_count = api_likes
            post.comments_count = api_comments_count
            post.synced_at = datetime.now(timezone.utc)
        else:
            comments_changed = api_comments_count > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="instagram",
                platform_post_id=platform_post_id,
                caption=m.get("caption", ""),
                media_url=m.get("media_url"),
                media_type=media_type_map.get(m.get("media_type"), "image"),
                thumbnail_url=m.get("thumbnail_url"),
                permalink=m.get("permalink"),
                likes_count=api_likes,
                comments_count=api_comments_count,
                posted_at=_parse_fb_time(m.get("timestamp")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        # Skip comment fetch if comments count unchanged or zero
        if not comments_changed or api_comments_count == 0:
            continue

        # Pre-load existing comment IDs for this post (one query instead of N)
        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id)
            .all()
        )

        # Fetch all comments for this post
        comments_data, _ = _paginate_graph_api(
            f"{api_base}/{platform_post_id}/comments",
            params={
                "access_token": token,
                "fields": "id,text,from,timestamp,like_count",
                "limit": 100,
            },
        )

        pending_contacts = []
        for c in comments_data:
            c_id = c.get("id", "")
            if c_id in existing_comment_ids:
                continue

            comment_text = c.get("text", "")
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)

            comment_from = c.get("from", {})
            author_id = comment_from.get("id", "")
            author_name = comment_from.get("username") or comment_from.get("name") or (f"User {author_id}" if author_id else "Unknown")

            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="instagram",
                platform_comment_id=c_id,
                platform_author_id=author_id or None,
                author_name=author_name,
                comment_text=comment_text,
                likes_count=c.get("like_count", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=_parse_fb_time(c.get("timestamp")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1

            if has_contact:
                pending_contacts.append(comment)

        # Flush once per post to get comment IDs, then extract contacts
        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    return stats


def sync_instagram_stream(page):
    """Generator: streams per-post progress events for Instagram sync (SSE-friendly)."""
    from app.services.ai_service import _heuristic_sentiment

    token = page.access_token
    if not token:
        yield {"type": "error", "error": "No access token for this page."}
        return

    api_base = _ig_api_base(token)
    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}
    media_type_map = {"IMAGE": "image", "VIDEO": "video", "CAROUSEL_ALBUM": "carousel"}

    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    media_data, err = _paginate_graph_api(
        f"{api_base}/{page.page_id}/media",
        params={
            "access_token": token,
            "fields": "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count",
            "limit": 100,
        },
    )
    if err and not media_data:
        yield {"type": "error", "error": f"Failed to fetch media: {err}"}
        return

    total_posts = len(media_data)
    yield {"type": "start", "total": total_posts}

    for idx, m in enumerate(media_data):
        platform_post_id = m.get("id", "")
        api_comments_count = m.get("comments_count", 0)
        api_likes = m.get("like_count", 0)
        caption = (m.get("caption") or "")[:60] or "(No caption)"
        thumbnail = m.get("thumbnail_url") or m.get("media_url")
        permalink = m.get("permalink")
        post_comments_synced = 0

        existing = existing_posts.get(platform_post_id)

        if existing:
            post = existing
            counts_changed = (
                post.likes_count != api_likes
                or post.comments_count != api_comments_count
            )
            if not counts_changed:
                db_comment_count = Comment.query.filter_by(post_id=post.id).count()
                if db_comment_count >= api_comments_count:
                    yield {"type": "post", "index": idx + 1, "total": total_posts,
                           "title": caption, "thumbnail": thumbnail, "permalink": permalink,
                           "likes": api_likes, "comments_synced": 0, "skipped": True}
                    continue

            comments_changed = post.comments_count != api_comments_count or (
                not counts_changed and Comment.query.filter_by(post_id=post.id).count() < api_comments_count
            )
            post.caption = m.get("caption") or post.caption
            post.media_url = m.get("media_url") or post.media_url
            post.permalink = m.get("permalink") or post.permalink
            post.likes_count = api_likes
            post.comments_count = api_comments_count
            post.synced_at = datetime.now(timezone.utc)
        else:
            comments_changed = api_comments_count > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="instagram",
                platform_post_id=platform_post_id,
                caption=m.get("caption", ""),
                media_url=m.get("media_url"),
                media_type=media_type_map.get(m.get("media_type"), "image"),
                thumbnail_url=m.get("thumbnail_url"),
                permalink=m.get("permalink"),
                likes_count=api_likes,
                comments_count=api_comments_count,
                posted_at=_parse_fb_time(m.get("timestamp")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        if not comments_changed or api_comments_count == 0:
            yield {"type": "post", "index": idx + 1, "total": total_posts,
                   "title": caption, "thumbnail": thumbnail, "permalink": permalink,
                   "likes": api_likes, "comments_synced": 0}
            continue

        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id)
            .all()
        )

        post_comments_data, _ = _paginate_graph_api(
            f"{api_base}/{platform_post_id}/comments",
            params={
                "access_token": token,
                "fields": "id,text,from,timestamp,like_count",
                "limit": 100,
            },
        )

        pending_contacts = []
        for c in post_comments_data:
            c_id = c.get("id", "")
            if c_id in existing_comment_ids:
                continue
            comment_text = c.get("text", "")
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)
            comment_from = c.get("from", {})
            author_id = comment_from.get("id", "")
            author_name = comment_from.get("username") or comment_from.get("name") or (
                f"User {author_id}" if author_id else "Unknown"
            )
            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="instagram",
                platform_comment_id=c_id,
                platform_author_id=author_id or None,
                author_name=author_name,
                comment_text=comment_text,
                likes_count=c.get("like_count", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=_parse_fb_time(c.get("timestamp")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1
            post_comments_synced += 1
            if has_contact:
                pending_contacts.append(comment)

        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

        yield {"type": "post", "index": idx + 1, "total": total_posts,
               "title": caption, "thumbnail": thumbnail, "permalink": permalink,
               "likes": api_likes, "comments_synced": post_comments_synced}

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    yield {"type": "done", **stats}


# ── YouTube sync ─────────────────────────────────────────────

def _sync_youtube_core(page, service, video_objects=None):
    """Core YouTube sync logic. Yields progress dicts for each video, then a final summary.

    Uses fast heuristic sentiment during sync for speed. AI sentiment analysis
    can be triggered separately on individual comments via the comments UI.
    """
    from app.services.ai_service import _heuristic_sentiment

    channel_id = page.page_id
    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}

    # Use provided video objects or fetch all from channel
    if video_objects is not None:
        videos = video_objects
    else:
        videos = service.fetch_all_channel_videos(channel_id)
    if not videos:
        yield {"type": "error", "error": "No videos found or API request failed. Check your API key and channel ID."}
        return

    total_videos = len(videos)
    yield {"type": "start", "total": total_videos}

    # Pre-load existing posts for this page (avoids per-video DB query)
    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    # ── Batch-fetch ALL video statistics up-front (50 IDs per request) ──
    # This replaces N individual stats API calls with ceil(N/50) calls —
    # for 74 videos: 74 calls → 2 calls.
    def _extract_video_id(v):
        if isinstance(v.get("id"), dict):
            return v["id"].get("videoId")
        if v.get("snippet", {}).get("resourceId"):
            return v["snippet"]["resourceId"].get("videoId")
        return v.get("id")

    all_video_ids = [_extract_video_id(v) for v in videos]
    all_video_ids = [vid for vid in all_video_ids if vid]

    all_video_stats = {}
    _BATCH = 50
    for _i in range(0, len(all_video_ids), _BATCH):
        _batch_ids = all_video_ids[_i:_i + _BATCH]
        try:
            _sr = http_requests.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={
                    "key": service.credentials.api_key,
                    "id": ",".join(_batch_ids),
                    "part": "statistics",
                },
                timeout=30,
            )
            for _item in _sr.json().get("items", []):
                all_video_stats[_item["id"]] = _item.get("statistics", {})
        except Exception:
            pass

    for idx, v in enumerate(videos):
        video_id = _extract_video_id(v)
        if not video_id:
            continue

        snippet = v.get("snippet", {})
        platform_post_id = video_id

        # Use pre-fetched stats (no extra API call per video)
        video_stats = all_video_stats.get(video_id, {})

        likes_count = int(video_stats.get("likeCount", 0))
        comments_count_api = int(video_stats.get("commentCount", 0))
        shares_count = int(video_stats.get("shareCount", 0))
        views_count = int(video_stats.get("viewCount", 0))
        permalink = f"https://www.youtube.com/watch?v={video_id}"
        thumbnail = (snippet.get("thumbnails") or {}).get("high", {}).get("url") or \
                     (snippet.get("thumbnails") or {}).get("default", {}).get("url")

        existing = existing_posts.get(platform_post_id)

        if existing:
            post = existing
            # Incremental: skip if nothing changed
            counts_changed = (
                post.likes_count != likes_count
                or post.comments_count != comments_count_api
                or post.shares_count != shares_count
                or post.views != views_count
            )
            if not counts_changed:
                # Yield progress even for skipped videos
                yield {
                    "type": "video",
                    "index": idx + 1,
                    "total": total_videos,
                    "video_id": video_id,
                    "title": snippet.get("title", "Untitled"),
                    "thumbnail": thumbnail,
                    "permalink": permalink,
                    "views": views_count,
                    "likes": likes_count,
                    "comments_synced": 0,
                    "comments_total": comments_count_api,
                    "skipped": True,
                }
                continue

            comments_changed = post.comments_count != comments_count_api

            post.caption = snippet.get("title") or post.caption
            post.media_url = thumbnail or post.media_url
            post.permalink = permalink
            post.likes_count = likes_count
            post.comments_count = comments_count_api
            post.shares_count = shares_count
            post.views = views_count
            post.synced_at = datetime.now(timezone.utc)
        else:
            comments_changed = comments_count_api > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="youtube",
                platform_post_id=platform_post_id,
                caption=snippet.get("title", ""),
                media_url=thumbnail,
                media_type="video",
                thumbnail_url=thumbnail,
                permalink=permalink,
                likes_count=likes_count,
                comments_count=comments_count_api,
                shares_count=shares_count,
                views=views_count,
                posted_at=_parse_yt_time(snippet.get("publishedAt")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        # Fetch comments only if comments count changed
        video_comments_synced = 0
        if comments_changed and comments_count_api > 0:
            # Pre-load existing comment IDs for this post (avoids per-comment DB query)
            existing_comment_ids = set(
                c.platform_comment_id
                for c in Comment.query.filter_by(post_id=post.id)
                .with_entities(Comment.platform_comment_id)
                .all()
            )

            pending_contacts = []
            comments_list = service.fetch_comments(video_id, include_replies=True)

            def _save_yt_comment(c_id, c_snippet, is_reply=False):
                """Helper: build and add a Comment row; return it or None if skipped."""
                if not c_id or c_id in existing_comment_ids:
                    return None
                comment_text = c_snippet.get("textOriginal", c_snippet.get("textDisplay", ""))
                yt_author_id = (c_snippet.get("authorChannelId") or {}).get("value", "")
                author_name = c_snippet.get("authorDisplayName") or (
                    f"User {yt_author_id}" if yt_author_id else "Unknown"
                )
                has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
                sentiment_result = _heuristic_sentiment(comment_text)
                comment = Comment(
                    post_id=post.id,
                    company_id=page.company_id,
                    platform="youtube",
                    platform_comment_id=c_id,
                    platform_author_id=yt_author_id or None,
                    author_name=author_name,
                    comment_text=comment_text,
                    likes_count=int(c_snippet.get("likeCount", 0)),
                    sentiment=sentiment_result.get("sentiment", "neutral"),
                    sentiment_score=sentiment_result.get("score", 0.5),
                    has_contact_info=has_contact,
                    commented_at=_parse_yt_time(c_snippet.get("publishedAt")),
                    synced_at=datetime.now(timezone.utc),
                )
                db.session.add(comment)
                existing_comment_ids.add(c_id)
                return comment

            for item in comments_list:
                # ── Top-level comment ──
                top_comment = item.get("snippet", {}).get("topLevelComment", {})
                c_id = top_comment.get("id", "")
                c_snippet = top_comment.get("snippet", {})
                saved = _save_yt_comment(c_id, c_snippet)
                if saved:
                    stats["comments_synced"] += 1
                    video_comments_synced += 1
                    if saved.has_contact_info:
                        pending_contacts.append(saved)

                # ── Inline replies (up to ~20, returned free with part=replies) ──
                thread_id = top_comment.get("id", "")
                total_replies = (item.get("snippet") or {}).get("totalReplyCount", 0)
                inline_replies = (item.get("replies") or {}).get("comments", [])

                for reply in inline_replies:
                    r_id = reply.get("id", "")
                    r_snippet = (reply.get("snippet") or {})
                    saved_r = _save_yt_comment(r_id, r_snippet, is_reply=True)
                    if saved_r:
                        stats["comments_synced"] += 1
                        video_comments_synced += 1
                        if saved_r.has_contact_info:
                            pending_contacts.append(saved_r)

                # ── Fetch remaining replies when thread has more than inline ──
                if thread_id and total_replies > len(inline_replies):
                    extra_replies = service.fetch_comment_replies(thread_id)
                    for reply in extra_replies:
                        r_id = reply.get("id", "")
                        r_snippet = (reply.get("snippet") or {})
                        saved_r = _save_yt_comment(r_id, r_snippet, is_reply=True)
                        if saved_r:
                            stats["comments_synced"] += 1
                            video_comments_synced += 1
                            if saved_r.has_contact_info:
                                pending_contacts.append(saved_r)

            # Flush once per video, then extract contacts
            if pending_contacts:
                db.session.flush()
                for comment in pending_contacts:
                    _extract_contact(comment, page.company_id, post.id)
                    stats["contacts_found"] += 1

        # Commit after every video to keep memory low and persist incremental progress
        _safe_commit()

        # Yield progress for this video
        yield {
            "type": "video",
            "index": idx + 1,
            "total": total_videos,
            "video_id": video_id,
            "title": snippet.get("title", "Untitled"),
            "thumbnail": thumbnail,
            "permalink": permalink,
            "views": views_count,
            "likes": likes_count,
            "comments_synced": video_comments_synced,
            "comments_total": comments_count_api,
        }

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()

    yield {"type": "done", **stats}


def _sync_youtube(page):
    """Fetch videos and comments from a YouTube channel (non-streaming)."""
    service = get_service(page.company_id, "youtube")
    if not service or not service.is_configured:
        return {"error": "YouTube API key not configured. Go to Admin > API Keys to configure."}
    if not page.page_id:
        return {"error": "No channel ID set for this page."}

    result = {}
    for event in _sync_youtube_core(page, service):
        if event["type"] == "error":
            return {"error": event["error"]}
        if event["type"] == "done":
            result = event
    return result


def sync_youtube_stream(page, video_objects=None):
    """Streaming YouTube sync — yields progress dicts for each video."""
    service = get_service(page.company_id, "youtube")
    if not service or not service.is_configured:
        yield {"type": "error", "error": "YouTube API key not configured. Go to Admin > API Keys to configure."}
        return
    if not page.page_id:
        yield {"type": "error", "error": "No channel ID set for this page."}
        return

    yield from _sync_youtube_core(page, service, video_objects=video_objects)


def _parse_yt_time(time_str):
    """Parse YouTube ISO 8601 timestamp (e.g. 2024-01-15T12:30:00Z)."""
    if not time_str:
        return None
    try:
        return datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


# ── LinkedIn sync ─────────────────────────────────────────────

def _sync_linkedin(page):
    """Fetch posts and comments from a LinkedIn organization page."""
    from app.services.ai_service import _heuristic_sentiment

    # Prefer page-level OAuth token; fall back to company API key token
    token = page.access_token
    if not token:
        key = CompanyAPIKey.get_for_company(page.company_id, "linkedin")
        if key and key.access_token:
            token = key.access_token
    if not token:
        return {"error": "No access token for this LinkedIn page. Please reconnect it via Pages."}

    org_id = page.page_id
    if not org_id:
        return {"error": "No LinkedIn organization ID set for this page."}

    LI_BASE = "https://api.linkedin.com/v2"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Restli-Protocol-Version": "2.0.0",
    }
    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}

    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    # Fetch organization posts (ugcPosts API)
    try:
        resp = http_requests.get(
            f"{LI_BASE}/ugcPosts",
            headers=headers,
            params={
                "q": "authors",
                "authors": f"List(urn:li:organization:{org_id})",
                "count": 50,
                "sortBy": "LAST_MODIFIED",
            },
            timeout=30,
        )
    except Exception as e:
        return {"error": f"Failed to fetch LinkedIn posts: {e}"}

    if resp.status_code == 401:
        return {"error": "LinkedIn access token expired. Please reconnect your LinkedIn page."}
    if resp.status_code == 403:
        return {
            "error": (
                "LinkedIn API access denied. Your app may need the "
                "'r_organization_social' permission (requires LinkedIn approval)."
            )
        }

    data = resp.json()
    if "serviceErrorCode" in data or (resp.status_code >= 400 and "message" in data):
        return {"error": f"LinkedIn API error: {data.get('message', 'Unknown error')}"}

    elements = data.get("elements", [])

    for p in elements:
        post_urn = p.get("id", "")
        if not post_urn:
            continue

        specific = p.get("specificContent", {}).get("com.linkedin.ugc.ShareContent", {})
        caption = specific.get("shareCommentary", {}).get("text", "")
        media_url = None
        media_list = specific.get("media", [])
        if media_list:
            media_url = media_list[0].get("originalUrl") or None

        created_ms = (p.get("created") or {}).get("time", 0)
        posted_at = (
            datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
            if created_ms else None
        )

        # Fetch engagement stats
        likes_count = 0
        comments_count_api = 0
        try:
            stats_resp = http_requests.get(
                f"{LI_BASE}/socialActions/{post_urn}",
                headers=headers,
                timeout=10,
            )
            social = stats_resp.json()
            likes_count = (social.get("likesSummary") or {}).get("totalLikes", 0)
            comments_count_api = (social.get("commentsSummary") or {}).get(
                "totalFirstLevelComments", 0
            )
        except Exception:
            pass

        existing = existing_posts.get(post_urn)

        if existing:
            post = existing
            counts_changed = (
                post.likes_count != likes_count
                or post.comments_count != comments_count_api
            )
            if not counts_changed:
                db_count = Comment.query.filter_by(post_id=post.id).count()
                if db_count >= comments_count_api:
                    continue
            comments_changed = post.comments_count != comments_count_api or (
                not counts_changed
                and Comment.query.filter_by(post_id=post.id).count() < comments_count_api
            )
            post.caption = caption or post.caption
            post.likes_count = likes_count
            post.comments_count = comments_count_api
            post.synced_at = datetime.now(timezone.utc)
        else:
            comments_changed = comments_count_api > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="linkedin",
                platform_post_id=post_urn,
                caption=caption,
                media_url=media_url,
                media_type="image" if media_url else "text",
                likes_count=likes_count,
                comments_count=comments_count_api,
                posted_at=posted_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        if not comments_changed or comments_count_api == 0:
            continue

        # Fetch comments for this post
        comments_data = []
        try:
            c_resp = http_requests.get(
                f"{LI_BASE}/socialActions/{post_urn}/comments",
                headers=headers,
                params={"count": 100},
                timeout=15,
            )
            if c_resp.status_code not in (403, 401):
                comments_data = c_resp.json().get("elements", [])
        except Exception:
            pass

        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id)
            .all()
        )

        pending_contacts = []
        for c in comments_data:
            c_id = c.get("id", "")
            if not c_id or c_id in existing_comment_ids:
                continue

            actor_urn = c.get("actor", "")
            comment_text = (c.get("message") or {}).get("text", "")
            c_created_ms = (c.get("created") or {}).get("time", 0)
            commented_at = (
                datetime.fromtimestamp(c_created_ms / 1000, tz=timezone.utc)
                if c_created_ms else None
            )
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)

            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="linkedin",
                platform_comment_id=c_id,
                platform_author_id=actor_urn or None,
                author_name=actor_urn.split(":")[-1] if actor_urn else "Unknown",
                comment_text=comment_text,
                likes_count=(c.get("likesSummary") or {}).get("totalLikes", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=commented_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1

            if has_contact:
                pending_contacts.append(comment)

        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    return stats


def sync_linkedin_stream(page):
    """Generator: streams per-post progress events for LinkedIn sync (SSE-friendly)."""
    from app.services.ai_service import _heuristic_sentiment

    token = page.access_token
    if not token:
        key = CompanyAPIKey.get_for_company(page.company_id, "linkedin")
        if key and key.access_token:
            token = key.access_token
    if not token:
        yield {"type": "error", "error": "No access token for this LinkedIn page. Please reconnect it via Pages."}
        return

    org_id = page.page_id
    if not org_id:
        yield {"type": "error", "error": "No LinkedIn organization ID set for this page."}
        return

    LI_BASE = "https://api.linkedin.com/v2"
    headers = {"Authorization": f"Bearer {token}", "X-Restli-Protocol-Version": "2.0.0"}
    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}

    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    try:
        resp = http_requests.get(
            f"{LI_BASE}/ugcPosts",
            headers=headers,
            params={"q": "authors", "authors": f"List(urn:li:organization:{org_id})",
                    "count": 50, "sortBy": "LAST_MODIFIED"},
            timeout=30,
        )
    except Exception as e:
        yield {"type": "error", "error": f"Failed to fetch LinkedIn posts: {e}"}
        return

    if resp.status_code == 401:
        yield {"type": "error", "error": "LinkedIn access token expired. Please reconnect your LinkedIn page."}
        return
    if resp.status_code == 403:
        yield {"type": "error", "error": "LinkedIn API access denied. Your app may need the 'r_organization_social' permission."}
        return

    data = resp.json()
    if "serviceErrorCode" in data or (resp.status_code >= 400 and "message" in data):
        yield {"type": "error", "error": f"LinkedIn API error: {data.get('message', 'Unknown error')}"}
        return

    elements = data.get("elements", [])
    total_posts = len(elements)
    yield {"type": "start", "total": total_posts}

    for idx, p in enumerate(elements):
        post_urn = p.get("id", "")
        if not post_urn:
            continue

        specific = p.get("specificContent", {}).get("com.linkedin.ugc.ShareContent", {})
        caption = (specific.get("shareCommentary", {}).get("text", "") or "")[:60] or "(No content)"
        media_url = None
        media_list = specific.get("media", [])
        if media_list:
            media_url = media_list[0].get("originalUrl") or None

        created_ms = (p.get("created") or {}).get("time", 0)
        posted_at = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc) if created_ms else None

        likes_count = 0
        comments_count_api = 0
        try:
            stats_resp = http_requests.get(f"{LI_BASE}/socialActions/{post_urn}", headers=headers, timeout=10)
            social = stats_resp.json()
            likes_count = (social.get("likesSummary") or {}).get("totalLikes", 0)
            comments_count_api = (social.get("commentsSummary") or {}).get("totalFirstLevelComments", 0)
        except Exception:
            pass

        post_comments_synced = 0
        existing = existing_posts.get(post_urn)

        if existing:
            post = existing
            counts_changed = (post.likes_count != likes_count or post.comments_count != comments_count_api)
            if not counts_changed:
                db_count = Comment.query.filter_by(post_id=post.id).count()
                if db_count >= comments_count_api:
                    yield {"type": "post", "index": idx + 1, "total": total_posts,
                           "title": caption, "thumbnail": media_url, "permalink": None,
                           "likes": likes_count, "comments_synced": 0, "skipped": True}
                    continue
            comments_changed = post.comments_count != comments_count_api or (
                not counts_changed and Comment.query.filter_by(post_id=post.id).count() < comments_count_api
            )
            post.caption = caption or post.caption
            post.likes_count = likes_count
            post.comments_count = comments_count_api
            post.synced_at = datetime.now(timezone.utc)
        else:
            comments_changed = comments_count_api > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="linkedin",
                platform_post_id=post_urn,
                caption=caption,
                media_url=media_url,
                media_type="image" if media_url else "text",
                likes_count=likes_count,
                comments_count=comments_count_api,
                posted_at=posted_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        if not comments_changed or comments_count_api == 0:
            yield {"type": "post", "index": idx + 1, "total": total_posts,
                   "title": caption, "thumbnail": media_url, "permalink": None,
                   "likes": likes_count, "comments_synced": 0}
            continue

        comments_data_li = []
        try:
            c_resp = http_requests.get(
                f"{LI_BASE}/socialActions/{post_urn}/comments",
                headers=headers, params={"count": 100}, timeout=15,
            )
            if c_resp.status_code not in (403, 401):
                comments_data_li = c_resp.json().get("elements", [])
        except Exception:
            pass

        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id).all()
        )

        pending_contacts = []
        for c in comments_data_li:
            c_id = c.get("id", "")
            if not c_id or c_id in existing_comment_ids:
                continue
            actor_urn = c.get("actor", "")
            comment_text = (c.get("message") or {}).get("text", "")
            c_created_ms = (c.get("created") or {}).get("time", 0)
            commented_at = datetime.fromtimestamp(c_created_ms / 1000, tz=timezone.utc) if c_created_ms else None
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)
            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="linkedin",
                platform_comment_id=c_id,
                platform_author_id=actor_urn or None,
                author_name=actor_urn.split(":")[-1] if actor_urn else "Unknown",
                comment_text=comment_text,
                likes_count=(c.get("likesSummary") or {}).get("totalLikes", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=commented_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1
            post_comments_synced += 1
            if has_contact:
                pending_contacts.append(comment)

        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

        yield {"type": "post", "index": idx + 1, "total": total_posts,
               "title": caption, "thumbnail": media_url, "permalink": None,
               "likes": likes_count, "comments_synced": post_comments_synced}

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    yield {"type": "done", **stats}


# ── Twitter/X sync ────────────────────────────────────────────

def _sync_twitter(page):
    """Fetch tweets and replies from a Twitter/X account."""
    from app.services.ai_service import _heuristic_sentiment

    # Bearer token from company API key
    key = CompanyAPIKey.get_for_company(page.company_id, "twitter")
    bearer_token = None
    if key:
        bearer_token = key.access_token or key.api_key
    if not bearer_token:
        return {"error": "Twitter Bearer token not configured. Go to Admin > API Keys."}

    user_id = page.page_id
    if not user_id:
        return {"error": "No Twitter user ID set for this page."}

    TW_BASE = "https://api.twitter.com/2"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}

    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    # Fetch recent tweets (up to 100; Twitter free tier allows this)
    all_tweets = []
    next_token = None
    for _ in range(10):  # up to 10 pages × 100 = 1,000 tweets
        try:
            params = {
                "max_results": 100,
                "tweet.fields": "created_at,public_metrics,text,conversation_id",
            }
            if next_token:
                params["pagination_token"] = next_token
            resp = http_requests.get(
                f"{TW_BASE}/users/{user_id}/tweets",
                headers=headers,
                params=params,
                timeout=30,
            )
            data = resp.json()
        except Exception as e:
            return {"error": f"Failed to fetch tweets: {e}"}

        if resp.status_code == 401:
            return {"error": "Twitter Bearer token invalid or expired."}
        if resp.status_code == 403:
            return {"error": "Twitter API access denied. Check your app permissions and plan."}
        if "title" in data and "data" not in data:
            return {"error": f"Twitter API error: {data.get('detail', data.get('title', 'Unknown'))}"}

        batch = data.get("data", [])
        if not batch:
            break
        all_tweets.extend(batch)
        next_token = (data.get("meta") or {}).get("next_token")
        if not next_token:
            break

    for t in all_tweets:
        tweet_id = t.get("id", "")
        if not tweet_id:
            continue

        metrics = t.get("public_metrics") or {}
        likes_count = metrics.get("like_count", 0)
        retweets = metrics.get("retweet_count", 0)
        reply_count = metrics.get("reply_count", 0)
        text = t.get("text", "")
        permalink = f"https://twitter.com/i/web/status/{tweet_id}"

        posted_at = None
        if t.get("created_at"):
            try:
                posted_at = datetime.fromisoformat(t["created_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        existing = existing_posts.get(tweet_id)

        if existing:
            post = existing
            counts_changed = (
                post.likes_count != likes_count
                or post.comments_count != reply_count
                or post.shares_count != retweets
            )
            if not counts_changed:
                db_count = Comment.query.filter_by(post_id=post.id).count()
                if db_count >= reply_count:
                    continue
            comments_changed = post.comments_count != reply_count or (
                not counts_changed
                and Comment.query.filter_by(post_id=post.id).count() < reply_count
            )
            post.caption = text
            post.likes_count = likes_count
            post.shares_count = retweets
            post.comments_count = reply_count
            post.synced_at = datetime.now(timezone.utc)
        else:
            comments_changed = reply_count > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="twitter",
                platform_post_id=tweet_id,
                caption=text,
                media_type="text",
                permalink=permalink,
                likes_count=likes_count,
                shares_count=retweets,
                comments_count=reply_count,
                posted_at=posted_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        if not comments_changed or reply_count == 0:
            continue

        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id)
            .all()
        )

        # Fetch replies via recent search (7-day window on free tier)
        replies = []
        user_lookup = {}
        try:
            r_resp = http_requests.get(
                f"{TW_BASE}/tweets/search/recent",
                headers=headers,
                params={
                    "query": f"conversation_id:{tweet_id} -from:{user_id}",
                    "max_results": min(100, max(reply_count + 10, 10)),
                    "tweet.fields": "created_at,author_id,text,public_metrics",
                    "expansions": "author_id",
                    "user.fields": "name,username",
                },
                timeout=15,
            )
            r_data = r_resp.json()
            replies = r_data.get("data", [])
            for u in (r_data.get("includes") or {}).get("users", []):
                user_lookup[u["id"]] = u.get("name") or u.get("username") or "Unknown"
        except Exception:
            pass

        pending_contacts = []
        for r in replies:
            r_id = r.get("id", "")
            if not r_id or r_id in existing_comment_ids:
                continue

            author_id = r.get("author_id", "")
            author_name = user_lookup.get(author_id, f"User {author_id}" if author_id else "Unknown")
            comment_text = r.get("text", "")

            commented_at = None
            if r.get("created_at"):
                try:
                    commented_at = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    pass

            r_metrics = r.get("public_metrics") or {}
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)

            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="twitter",
                platform_comment_id=r_id,
                platform_author_id=author_id or None,
                author_name=author_name,
                comment_text=comment_text,
                likes_count=r_metrics.get("like_count", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=commented_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1

            if has_contact:
                pending_contacts.append(comment)

        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    return stats


def sync_twitter_stream(page):
    """Generator: streams per-tweet progress events for Twitter sync (SSE-friendly)."""
    from app.services.ai_service import _heuristic_sentiment

    key = CompanyAPIKey.get_for_company(page.company_id, "twitter")
    bearer_token = None
    if key:
        bearer_token = key.access_token or key.api_key
    if not bearer_token:
        yield {"type": "error", "error": "Twitter Bearer token not configured. Go to Admin > API Keys."}
        return

    user_id = page.page_id
    if not user_id:
        yield {"type": "error", "error": "No Twitter user ID set for this page."}
        return

    TW_BASE = "https://api.twitter.com/2"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    stats = {"posts_synced": 0, "comments_synced": 0, "contacts_found": 0}

    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    all_tweets = []
    next_token = None
    for _ in range(10):
        try:
            params = {"max_results": 100, "tweet.fields": "created_at,public_metrics,text,conversation_id"}
            if next_token:
                params["pagination_token"] = next_token
            resp = http_requests.get(
                f"{TW_BASE}/users/{user_id}/tweets",
                headers=headers, params=params, timeout=30,
            )
            data = resp.json()
        except Exception as e:
            yield {"type": "error", "error": f"Failed to fetch tweets: {e}"}
            return

        if resp.status_code == 401:
            yield {"type": "error", "error": "Twitter Bearer token invalid or expired."}
            return
        if resp.status_code == 403:
            yield {"type": "error", "error": "Twitter API access denied. Check your app permissions and plan."}
            return
        if "title" in data and "data" not in data:
            yield {"type": "error", "error": f"Twitter API error: {data.get('detail', data.get('title', 'Unknown'))}"}
            return

        batch = data.get("data", [])
        if not batch:
            break
        all_tweets.extend(batch)
        next_token = (data.get("meta") or {}).get("next_token")
        if not next_token:
            break

    total_posts = len(all_tweets)
    yield {"type": "start", "total": total_posts}

    for idx, t in enumerate(all_tweets):
        tweet_id = t.get("id", "")
        if not tweet_id:
            continue

        metrics = t.get("public_metrics") or {}
        likes_count = metrics.get("like_count", 0)
        retweets = metrics.get("retweet_count", 0)
        reply_count = metrics.get("reply_count", 0)
        text = t.get("text", "")
        caption = text[:60] or "(No text)"
        permalink = f"https://twitter.com/i/web/status/{tweet_id}"
        post_comments_synced = 0

        posted_at = None
        if t.get("created_at"):
            try:
                posted_at = datetime.fromisoformat(t["created_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        existing = existing_posts.get(tweet_id)

        if existing:
            post = existing
            counts_changed = (
                post.likes_count != likes_count
                or post.comments_count != reply_count
                or post.shares_count != retweets
            )
            if not counts_changed:
                db_count = Comment.query.filter_by(post_id=post.id).count()
                if db_count >= reply_count:
                    yield {"type": "post", "index": idx + 1, "total": total_posts,
                           "title": caption, "thumbnail": None, "permalink": permalink,
                           "likes": likes_count, "comments_synced": 0, "skipped": True}
                    continue
            comments_changed = post.comments_count != reply_count or (
                not counts_changed and Comment.query.filter_by(post_id=post.id).count() < reply_count
            )
            post.caption = text
            post.likes_count = likes_count
            post.shares_count = retweets
            post.comments_count = reply_count
            post.synced_at = datetime.now(timezone.utc)
        else:
            comments_changed = reply_count > 0
            post = Post(
                company_id=page.company_id,
                connected_page_id=page.id,
                platform="twitter",
                platform_post_id=tweet_id,
                caption=text,
                media_type="text",
                permalink=permalink,
                likes_count=likes_count,
                shares_count=retweets,
                comments_count=reply_count,
                posted_at=posted_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

        if not comments_changed or reply_count == 0:
            yield {"type": "post", "index": idx + 1, "total": total_posts,
                   "title": caption, "thumbnail": None, "permalink": permalink,
                   "likes": likes_count, "comments_synced": 0}
            continue

        existing_comment_ids = set(
            c.platform_comment_id
            for c in Comment.query.filter_by(post_id=post.id)
            .with_entities(Comment.platform_comment_id).all()
        )

        replies = []
        user_lookup = {}
        try:
            r_resp = http_requests.get(
                f"{TW_BASE}/tweets/search/recent",
                headers=headers,
                params={
                    "query": f"conversation_id:{tweet_id} -from:{user_id}",
                    "max_results": min(100, max(reply_count + 10, 10)),
                    "tweet.fields": "created_at,author_id,text,public_metrics",
                    "expansions": "author_id",
                    "user.fields": "name,username",
                },
                timeout=15,
            )
            r_data = r_resp.json()
            replies = r_data.get("data", [])
            for u in (r_data.get("includes") or {}).get("users", []):
                user_lookup[u["id"]] = u.get("name") or u.get("username") or "Unknown"
        except Exception:
            pass

        pending_contacts = []
        for r in replies:
            r_id = r.get("id", "")
            if not r_id or r_id in existing_comment_ids:
                continue
            author_id = r.get("author_id", "")
            author_name = user_lookup.get(author_id, f"User {author_id}" if author_id else "Unknown")
            comment_text = r.get("text", "")
            commented_at = None
            if r.get("created_at"):
                try:
                    commented_at = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    pass
            r_metrics = r.get("public_metrics") or {}
            has_contact = bool(PHONE_RE.search(comment_text) or EMAIL_RE.search(comment_text))
            sentiment_result = _heuristic_sentiment(comment_text)
            comment = Comment(
                post_id=post.id,
                company_id=page.company_id,
                platform="twitter",
                platform_comment_id=r_id,
                platform_author_id=author_id or None,
                author_name=author_name,
                comment_text=comment_text,
                likes_count=r_metrics.get("like_count", 0),
                sentiment=sentiment_result.get("sentiment", "neutral"),
                sentiment_score=sentiment_result.get("score", 0.5),
                has_contact_info=has_contact,
                commented_at=commented_at,
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(comment)
            stats["comments_synced"] += 1
            post_comments_synced += 1
            if has_contact:
                pending_contacts.append(comment)

        if pending_contacts:
            db.session.flush()
            for comment in pending_contacts:
                _extract_contact(comment, page.company_id, post.id)
                stats["contacts_found"] += 1

        yield {"type": "post", "index": idx + 1, "total": total_posts,
               "title": caption, "thumbnail": None, "permalink": permalink,
               "likes": likes_count, "comments_synced": post_comments_synced}

    page.last_synced_at = datetime.now(timezone.utc)
    _safe_commit()
    yield {"type": "done", **stats}


# ── Generic stub sync (Google Reviews, etc.) ─────────────────

def _sync_generic(page):
    """Placeholder sync for unsupported platforms."""
    return {
        "error": f"Automatic sync for {page.platform} is not yet supported."
    }


# ── Main entry points ──────────────────────────────────────

def sync_page(page_id):
    """Sync posts and comments for a connected page.

    Returns dict with sync results summary.
    """
    page = ConnectedPage.query.get(page_id)
    if not page:
        return {"error": "Page not found"}

    if page.status != "connected":
        return {"error": "Page is not connected."}

    if page.platform == "facebook":
        return _sync_facebook(page)
    elif page.platform == "instagram":
        return _sync_instagram(page)
    elif page.platform == "youtube":
        return _sync_youtube(page)
    elif page.platform == "linkedin":
        return _sync_linkedin(page)
    elif page.platform == "twitter":
        return _sync_twitter(page)
    else:
        return _sync_generic(page)


def sync_all_pages(company_id):
    """Sync all active connected pages for a company."""
    pages = ConnectedPage.query.filter_by(company_id=company_id, status="connected").all()
    results = []
    for page in pages:
        result = sync_page(page.id)
        result["page_name"] = page.page_name
        result["platform"] = page.platform
        results.append(result)
    return results


# ── Helpers ─────────────────────────────────────────────────

def _parse_fb_time(time_str):
    """Parse Facebook/Instagram ISO 8601 timestamp."""
    if not time_str:
        return None
    try:
        # Facebook format: 2024-01-15T12:30:00+0000
        return datetime.fromisoformat(time_str.replace("+0000", "+00:00"))
    except (ValueError, AttributeError):
        try:
            return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S%z")
        except (ValueError, AttributeError):
            return None


def _is_valid_phone(text):
    """Check that a matched string is a real phone number, not a date."""
    text = text.strip()
    if DATE_RE.match(text):
        return False
    # Must contain at least 7 actual digits
    digits = re.sub(r"\D", "", text)
    return len(digits) >= 7


def _extract_contact(comment, company_id, post_id):
    """Extract phone/email from a comment and save as a Contact."""
    raw_phones = PHONE_RE.findall(comment.comment_text)
    phones = [p for p in raw_phones if _is_valid_phone(p)]
    emails = EMAIL_RE.findall(comment.comment_text)

    if phones or emails:
        contact = Contact(
            company_id=company_id,
            comment_id=comment.id,
            source_post_id=post_id,
            name=comment.author_name,
            phone=phones[0].strip() if phones else None,
            email=emails[0].strip() if emails else None,
            platform=comment.platform,
            contact_type="lead",
            notes=f"Auto-extracted from comment: {comment.comment_text[:200]}",
        )
        db.session.add(contact)

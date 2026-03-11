"""Unified sync orchestrator.

Routes sync requests to the correct platform service based on the
ConnectedPage's platform.  Uses the page's own access_token (from OAuth)
or falls back to company-level CompanyAPIKey credentials.
"""

import re
import requests as http_requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Reusable session for connection pooling (keeps TCP connections alive)
_http_session = http_requests.Session()
_http_session.headers.update({"Connection": "keep-alive"})
adapter = http_requests.adapters.HTTPAdapter(
    pool_connections=10, pool_maxsize=20, max_retries=2,
)
_http_session.mount("https://", adapter)
_http_session.mount("http://", adapter)


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


def _mark_owner_replies_for_page(page):
    """Detect comments replied to by the page owner on the platform.

    When the page owner (whose platform ID is page.page_id) has commented on
    a post, mark all other non-owner comments on that post as is_replied=True.
    This ensures the Response Tracker reflects replies made outside the app.
    """
    if not page.page_id:
        return

    # Find post IDs where the page owner has commented
    owner_post_ids = (
        db.session.query(Comment.post_id)
        .filter(
            Comment.company_id == page.company_id,
            Comment.platform_author_id == page.page_id,
        )
        .distinct()
        .subquery()
    )

    # Bulk-update: mark non-owner, unreplied comments on those posts
    Comment.query.filter(
        Comment.post_id.in_(db.session.query(owner_post_ids.c.post_id)),
        Comment.platform_author_id != page.page_id,
        Comment.is_replied == False,
    ).update({"is_replied": True}, synchronize_session="fetch")


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


def _paginate_graph_api(url, params, timeout=90):
    """Fetch all pages from a Facebook/Instagram Graph API endpoint.

    The Graph API returns paginated results with a 'paging.next' URL.
    This helper follows all pages and returns the combined 'data' list.
    Connection pooling and retries are handled by the shared _http_session.
    """
    all_data = []

    try:
        resp = _http_session.get(url, params=params, timeout=timeout)
        result = resp.json()
    except Exception as e:
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
            resp = _http_session.get(next_url, timeout=timeout)
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
    # Try with insights first; fall back without if the token lacks insights permission
    _fb_fields_with_insights = "id,message,created_time,full_picture,shares,permalink_url,likes.summary(true),comments.summary(true),insights.metric(post_impressions){values}"
    _fb_fields_without_insights = "id,message,created_time,full_picture,shares,permalink_url,likes.summary(true),comments.summary(true)"
    posts_data, err = _paginate_graph_api(
        f"{GRAPH_API_BASE}/{page.page_id}/posts",
        params={"access_token": token, "fields": _fb_fields_with_insights, "limit": 100},
    )
    if err and not posts_data:
        # Retry without insights (token may lack page insights permission)
        posts_data, err = _paginate_graph_api(
            f"{GRAPH_API_BASE}/{page.page_id}/posts",
            params={"access_token": token, "fields": _fb_fields_without_insights, "limit": 100},
        )
    if err and not posts_data:
        return {"error": f"Failed to fetch posts: {err}"}

    # ── Pre-fetch comments for posts that need them (parallel) ──
    def _fb_reg_needs_comments(p_data):
        pid = p_data.get("id", "")
        ac = (p_data.get("comments") or {}).get("summary", {}).get("total_count", 0)
        ex = existing_posts.get(pid)
        if ex:
            return ex.comments_count != ac and ac > 0
        return ac > 0

    fb_reg_needing = [p for p in posts_data if _fb_reg_needs_comments(p)]
    prefetched_fb_reg = {}
    if fb_reg_needing:
        def _fetch_fb_reg_comments(pid):
            data, _ = _paginate_graph_api(
                f"{GRAPH_API_BASE}/{pid}/comments",
                params={"access_token": token, "fields": "id,from,message,created_time,like_count",
                        "filter": "stream", "limit": 100},
            )
            return pid, data
        with ThreadPoolExecutor(max_workers=min(8, len(fb_reg_needing))) as executor:
            futures = {executor.submit(_fetch_fb_reg_comments, p.get("id", "")): p for p in fb_reg_needing}
            for future in as_completed(futures):
                try:
                    pid, cdata = future.result()
                    prefetched_fb_reg[pid] = cdata
                except Exception:
                    pass

    for p in posts_data:
        platform_post_id = p.get("id", "")

        # Parse engagement counts from Graph API summary objects
        api_likes = (p.get("likes") or {}).get("summary", {}).get("total_count", 0)
        api_comments = (p.get("comments") or {}).get("summary", {}).get("total_count", 0)
        api_shares = (p.get("shares") or {}).get("count", 0)
        # Parse views from post insights (post_impressions)
        api_views = 0
        for insight in (p.get("insights") or {}).get("data", []):
            if insight.get("name") == "post_impressions":
                api_views = (insight.get("values") or [{}])[0].get("value", 0)
                break

        existing = existing_posts.get(platform_post_id)

        if existing:
            post = existing
            # Incremental: skip if nothing changed AND all comments already synced
            counts_changed = (
                post.likes_count != api_likes
                or post.comments_count != api_comments
                or post.shares_count != api_shares
                or post.views != api_views
            )
            if not counts_changed:
                db_comment_count = Comment.query.filter_by(post_id=post.id).count()
                if db_comment_count >= api_comments:
                    continue  # truly nothing new

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
            post.views = api_views
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
                views=api_views,
                posted_at=_parse_fb_time(p.get("created_time")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()  # ensure post.id is set
        stats["posts_synced"] += 1

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

        # Use pre-fetched comments (already downloaded in parallel)
        comments_data = prefetched_fb_reg.get(platform_post_id) or []

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

    # Detect replies made by the page owner on the platform
    _mark_owner_replies_for_page(page)

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

    _fb_fields_with_insights = "id,message,created_time,full_picture,shares,permalink_url,likes.summary(true),comments.summary(true),insights.metric(post_impressions){values}"
    _fb_fields_without_insights = "id,message,created_time,full_picture,shares,permalink_url,likes.summary(true),comments.summary(true)"
    posts_data, err = _paginate_graph_api(
        f"{GRAPH_API_BASE}/{page.page_id}/posts",
        params={"access_token": token, "fields": _fb_fields_with_insights, "limit": 100},
    )
    if err and not posts_data:
        # Retry without insights (token may lack page insights permission)
        posts_data, err = _paginate_graph_api(
            f"{GRAPH_API_BASE}/{page.page_id}/posts",
            params={"access_token": token, "fields": _fb_fields_without_insights, "limit": 100},
        )
    if err and not posts_data:
        yield {"type": "error", "error": f"Failed to fetch posts: {err}"}
        return

    total_posts = len(posts_data)
    yield {"type": "start", "total": total_posts}

    # ── Pre-fetch comments for all posts that need them (parallel) ──
    def _fb_needs_comments(p_data):
        pid = p_data.get("id", "")
        ac = (p_data.get("comments") or {}).get("summary", {}).get("total_count", 0)
        ex = existing_posts.get(pid)
        if ex:
            return ex.comments_count != ac and ac > 0
        return ac > 0

    posts_needing_comments = [p for p in posts_data if _fb_needs_comments(p)]
    prefetched_fb_comments = {}
    if posts_needing_comments:
        def _fetch_fb_comments(pid):
            data, _ = _paginate_graph_api(
                f"{GRAPH_API_BASE}/{pid}/comments",
                params={"access_token": token, "fields": "id,from,message,created_time,like_count",
                        "filter": "stream", "limit": 100},
            )
            return pid, data
        with ThreadPoolExecutor(max_workers=min(8, len(posts_needing_comments))) as executor:
            futures = {executor.submit(_fetch_fb_comments, p.get("id", "")): p for p in posts_needing_comments}
            for future in as_completed(futures):
                try:
                    pid, cdata = future.result()
                    prefetched_fb_comments[pid] = cdata
                except Exception:
                    pass

    for idx, p in enumerate(posts_data):
        platform_post_id = p.get("id", "")
        api_likes = (p.get("likes") or {}).get("summary", {}).get("total_count", 0)
        api_comments = (p.get("comments") or {}).get("summary", {}).get("total_count", 0)
        api_shares = (p.get("shares") or {}).get("count", 0)
        api_views = 0
        for insight in (p.get("insights") or {}).get("data", []):
            if insight.get("name") == "post_impressions":
                api_views = (insight.get("values") or [{}])[0].get("value", 0)
                break
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
                or post.views != api_views
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
            post.views = api_views
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
                views=api_views,
                posted_at=_parse_fb_time(p.get("created_time")),
                synced_at=datetime.now(timezone.utc),
            )
            db.session.add(post)

        db.session.flush()
        stats["posts_synced"] += 1

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

        # Use pre-fetched comments (already downloaded in parallel)
        post_comments_data = prefetched_fb_comments.get(platform_post_id) or []

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

    _mark_owner_replies_for_page(page)
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
    # Try with insights first; fall back without if the token lacks insights permission
    _ig_fields_with = "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count,insights.metric(impressions){values}"
    _ig_fields_without = "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count"
    media_data, err = _paginate_graph_api(
        f"{api_base}/{page.page_id}/media",
        params={"access_token": token, "fields": _ig_fields_with, "limit": 100},
    )
    if err and not media_data:
        media_data, err = _paginate_graph_api(
            f"{api_base}/{page.page_id}/media",
            params={"access_token": token, "fields": _ig_fields_without, "limit": 100},
        )
    if err and not media_data:
        return {"error": f"Failed to fetch media: {err}"}

    # ── Pre-fetch comments for posts that need them (parallel) ──
    def _ig_reg_needs_comments(m_data):
        pid = m_data.get("id", "")
        ac = m_data.get("comments_count", 0)
        ex = existing_posts.get(pid)
        if ex:
            return ex.comments_count != ac and ac > 0
        return ac > 0

    ig_reg_needing = [m for m in media_data if _ig_reg_needs_comments(m)]
    prefetched_ig_reg = {}
    if ig_reg_needing:
        def _fetch_ig_reg_comments(pid):
            data, _ = _paginate_graph_api(
                f"{api_base}/{pid}/comments",
                params={"access_token": token, "fields": "id,text,from,timestamp,like_count",
                        "limit": 100},
            )
            return pid, data
        with ThreadPoolExecutor(max_workers=min(8, len(ig_reg_needing))) as executor:
            futures = {executor.submit(_fetch_ig_reg_comments, m.get("id", "")): m for m in ig_reg_needing}
            for future in as_completed(futures):
                try:
                    pid, cdata = future.result()
                    prefetched_ig_reg[pid] = cdata
                except Exception:
                    pass

    for m in media_data:
        platform_post_id = m.get("id", "")
        api_comments_count = m.get("comments_count", 0)
        api_likes = m.get("like_count", 0)
        # Parse views from insights (impressions)
        api_views = 0
        for insight in (m.get("insights") or {}).get("data", []):
            if insight.get("name") == "impressions":
                api_views = (insight.get("values") or [{}])[0].get("value", 0)
                break

        existing = existing_posts.get(platform_post_id)

        if existing:
            post = existing
            # Incremental: skip if nothing changed AND all comments already synced
            counts_changed = (
                post.likes_count != api_likes
                or post.comments_count != api_comments_count
                or post.views != api_views
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
            post.views = api_views
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
                views=api_views,
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

        # Use pre-fetched comments (already fetched in parallel above)
        comments_data = prefetched_ig_reg.get(platform_post_id, [])

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

    _mark_owner_replies_for_page(page)
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

    _ig_fields_with = "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count,insights.metric(impressions){values}"
    _ig_fields_without = "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count"
    media_data, err = _paginate_graph_api(
        f"{api_base}/{page.page_id}/media",
        params={"access_token": token, "fields": _ig_fields_with, "limit": 100},
    )
    if err and not media_data:
        media_data, err = _paginate_graph_api(
            f"{api_base}/{page.page_id}/media",
            params={"access_token": token, "fields": _ig_fields_without, "limit": 100},
        )
    if err and not media_data:
        yield {"type": "error", "error": f"Failed to fetch media: {err}"}
        return

    total_posts = len(media_data)
    yield {"type": "start", "total": total_posts}

    # ── Pre-fetch comments for all posts that need them (parallel) ──
    def _ig_needs_comments(m_data):
        pid = m_data.get("id", "")
        ac = m_data.get("comments_count", 0)
        ex = existing_posts.get(pid)
        if ex:
            return ex.comments_count != ac and ac > 0
        return ac > 0

    ig_posts_needing_comments = [m for m in media_data if _ig_needs_comments(m)]
    prefetched_ig_comments = {}
    if ig_posts_needing_comments:
        def _fetch_ig_comments(pid):
            data, _ = _paginate_graph_api(
                f"{api_base}/{pid}/comments",
                params={"access_token": token, "fields": "id,text,from,timestamp,like_count", "limit": 100},
            )
            return pid, data
        with ThreadPoolExecutor(max_workers=min(8, len(ig_posts_needing_comments))) as executor:
            futures = {executor.submit(_fetch_ig_comments, m.get("id", "")): m for m in ig_posts_needing_comments}
            for future in as_completed(futures):
                try:
                    pid, cdata = future.result()
                    prefetched_ig_comments[pid] = cdata
                except Exception:
                    pass

    for idx, m in enumerate(media_data):
        platform_post_id = m.get("id", "")
        api_comments_count = m.get("comments_count", 0)
        api_likes = m.get("like_count", 0)
        api_views = 0
        for insight in (m.get("insights") or {}).get("data", []):
            if insight.get("name") == "impressions":
                api_views = (insight.get("values") or [{}])[0].get("value", 0)
                break
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
                or post.views != api_views
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
            post.views = api_views
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
                views=api_views,
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

        # Use pre-fetched comments (already downloaded in parallel)
        post_comments_data = prefetched_ig_comments.get(platform_post_id) or []

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

    _mark_owner_replies_for_page(page)
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

    # Pre-load existing posts for this page
    existing_posts = {
        p.platform_post_id: p
        for p in Post.query.filter_by(connected_page_id=page.id).all()
    }

    def _extract_video_id(v):
        if isinstance(v.get("id"), dict):
            return v["id"].get("videoId")
        if v.get("snippet", {}).get("resourceId"):
            return v["snippet"]["resourceId"].get("videoId")
        return v.get("id")

    # Use provided video objects or fetch from channel
    if video_objects is not None:
        videos = video_objects
        yt_fetch_err = None
    elif existing_posts:
        # Re-sync: only fetch NEW videos (stop at first known video)
        yield {"type": "phase", "message": "Checking for new videos..."}
        videos, yt_fetch_err = service.fetch_all_channel_videos(
            channel_id, known_video_ids=set(existing_posts.keys())
        )
        # Even if no new videos, we still check stats for existing ones
        if yt_fetch_err and not videos and not existing_posts:
            yield {"type": "error", "error": f"YouTube API error: {yt_fetch_err}"}
            return
    else:
        # First sync: fetch ALL videos
        yield {"type": "phase", "message": "Fetching video list from channel..."}
        videos, yt_fetch_err = service.fetch_all_channel_videos(channel_id)
        if not videos:
            msg = "No videos found or API request failed."
            if yt_fetch_err:
                msg = f"YouTube API error: {yt_fetch_err}"
            yield {"type": "error", "error": msg}
            return

    # Build full video ID list: new videos from API + existing from DB
    new_video_ids = [_extract_video_id(v) for v in videos]
    new_video_ids = [vid for vid in new_video_ids if vid]
    all_video_ids = list(dict.fromkeys(new_video_ids + list(existing_posts.keys())))

    total_videos = len(all_video_ids)
    if total_videos == 0:
        yield {"type": "error", "error": "No videos found."}
        return
    yield {"type": "start", "total": total_videos}

    # Map new video snippets by ID for quick lookup
    new_video_snippets = {}
    for v in videos:
        vid = _extract_video_id(v)
        if vid:
            new_video_snippets[vid] = v.get("snippet", {})

    all_video_stats = {}
    _BATCH = 50
    _stat_batches = [all_video_ids[i:i + _BATCH] for i in range(0, len(all_video_ids), _BATCH)]

    def _fetch_stats_batch(batch_ids):
        result = {}
        try:
            _sr = _http_session.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={
                    "key": service.credentials.api_key,
                    "id": ",".join(batch_ids),
                    "part": "statistics",
                },
                timeout=30,
            )
            for _item in _sr.json().get("items", []):
                result[_item["id"]] = _item.get("statistics", {})
        except Exception:
            pass
        return result

    if len(_stat_batches) > 1:
        with ThreadPoolExecutor(max_workers=len(_stat_batches)) as executor:
            for batch_result in executor.map(_fetch_stats_batch, _stat_batches):
                all_video_stats.update(batch_result)
    elif _stat_batches:
        all_video_stats.update(_fetch_stats_batch(_stat_batches[0]))

    # ── Determine which videos need comment fetching ──
    videos_needing_comments = set()
    for v in videos:
        vid = _extract_video_id(v)
        if not vid:
            continue
        vs = all_video_stats.get(vid, {})
        existing = existing_posts.get(vid)
        if existing:
            cc_api = int(vs.get("commentCount", 0))
            if existing.comments_count != cc_api and cc_api > 0:
                videos_needing_comments.add(vid)
        elif int(vs.get("commentCount", 0)) > 0:
            videos_needing_comments.add(vid)

    # ── Launch comment pre-fetch in background (non-blocking) ──
    # Comments are fetched in parallel threads while we start processing
    # videos immediately.  Each video grabs its comments from the dict
    # when ready, or waits on its individual future.
    if videos_needing_comments:
        yield {"type": "phase", "message": f"Loading comments for {len(videos_needing_comments)} videos..."}
    comment_futures = {}
    _comment_executor = ThreadPoolExecutor(max_workers=min(16, max(len(videos_needing_comments), 1)))
    for vid in videos_needing_comments:
        comment_futures[vid] = _comment_executor.submit(
            lambda v=vid: service.fetch_comments(v, include_replies=True)
        )

    def _get_comments_for(vid):
        """Get comments for a video — from background pool or direct fetch."""
        fut = comment_futures.get(vid)
        if fut:
            try:
                return fut.result(timeout=120)
            except Exception:
                return []
        return service.fetch_comments(vid, include_replies=True)

    def _get_extra_replies_parallel(comments_list):
        """Fetch extra replies for threads that exceed inline limit, in parallel."""
        threads_need = []
        for item in comments_list:
            top = item.get("snippet", {}).get("topLevelComment", {})
            tid = top.get("id", "")
            total_r = (item.get("snippet") or {}).get("totalReplyCount", 0)
            inline_r = (item.get("replies") or {}).get("comments", [])
            if tid and total_r > len(inline_r):
                threads_need.append(tid)
        if not threads_need:
            return {}
        reply_map = {}
        with ThreadPoolExecutor(max_workers=min(16, len(threads_need))) as rexec:
            futs = {rexec.submit(service.fetch_comment_replies, tid): tid for tid in threads_need}
            for fut in as_completed(futs):
                tid = futs[fut]
                try:
                    reply_map[tid] = fut.result()
                except Exception:
                    pass
        return reply_map

    # ── Main loop — starts immediately, no blocking wait ──
    for idx, video_id in enumerate(all_video_ids):
        if not video_id:
            continue

        # Get snippet: from new API data, or from existing DB post
        snippet = new_video_snippets.get(video_id, {})
        existing = existing_posts.get(video_id)

        # Use pre-fetched stats (no extra API call per video)
        video_stats = all_video_stats.get(video_id, {})

        likes_count = int(video_stats.get("likeCount", 0))
        comments_count_api = int(video_stats.get("commentCount", 0))
        shares_count = int(video_stats.get("shareCount", 0))
        views_count = int(video_stats.get("viewCount", 0))
        permalink = f"https://www.youtube.com/watch?v={video_id}"
        thumbnail = (snippet.get("thumbnails") or {}).get("high", {}).get("url") or \
                     (snippet.get("thumbnails") or {}).get("default", {}).get("url") or \
                     (existing.thumbnail_url if existing else None)

        # Title: prefer API snippet, fall back to existing DB caption
        title = snippet.get("title") or (existing.caption if existing else "") or "Untitled"

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
                    "title": title,
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

            post.caption = title
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
                platform_post_id=video_id,
                caption=title,
                media_url=thumbnail,
                media_type="video",
                thumbnail_url=thumbnail,
                permalink=permalink,
                likes_count=likes_count,
                comments_count=comments_count_api,
                shares_count=shares_count,
                views=views_count,
                posted_at=_parse_yt_time(snippet.get("publishedAt")) if snippet else None,
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
            # Get comments (from background pool — may already be ready)
            comments_list = _get_comments_for(video_id)
            # Fetch extra replies for this video's threads in parallel
            extra_replies_map = _get_extra_replies_parallel(comments_list)

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

                # ── Extra replies from parallel pre-fetch ──
                if thread_id and total_replies > len(inline_replies):
                    extra_replies = extra_replies_map.get(thread_id, [])
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

        # Batch commit every 10 videos for speed (instead of per-video)
        if (idx + 1) % 10 == 0:
            _safe_commit()

        # Yield progress for this video
        yield {
            "type": "video",
            "index": idx + 1,
            "total": total_videos,
            "video_id": video_id,
            "title": title,
            "thumbnail": thumbnail,
            "permalink": permalink,
            "views": views_count,
            "likes": likes_count,
            "comments_synced": video_comments_synced,
            "comments_total": comments_count_api,
        }

    _comment_executor.shutdown(wait=False)

    _mark_owner_replies_for_page(page)
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
        resp = _http_session.get(
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

    # ── Pre-fetch stats AND comments for all posts in parallel ──
    prefetched_li_reg_stats = {}   # urn -> {likes, comments_count}
    prefetched_li_reg_comments = {}  # urn -> [comment_elements]

    def _fetch_li_reg_stats_and_comments(urn):
        li_stats = {"likes": 0, "comments_count": 0}
        li_comments = []
        try:
            stats_resp = _http_session.get(
                f"{LI_BASE}/socialActions/{urn}",
                headers=headers,
                timeout=10,
            )
            social = stats_resp.json()
            li_stats["likes"] = (social.get("likesSummary") or {}).get("totalLikes", 0)
            li_stats["comments_count"] = (social.get("commentsSummary") or {}).get(
                "totalFirstLevelComments", 0
            )
        except Exception:
            pass
        try:
            c_resp = _http_session.get(
                f"{LI_BASE}/socialActions/{urn}/comments",
                headers=headers,
                params={"count": 100},
                timeout=15,
            )
            if c_resp.status_code not in (403, 401):
                li_comments = c_resp.json().get("elements", [])
        except Exception:
            pass
        return urn, li_stats, li_comments

    urns_to_fetch = [p.get("id", "") for p in elements if p.get("id")]
    if urns_to_fetch:
        with ThreadPoolExecutor(max_workers=min(8, len(urns_to_fetch))) as executor:
            futures = {executor.submit(_fetch_li_reg_stats_and_comments, urn): urn for urn in urns_to_fetch}
            for future in as_completed(futures):
                try:
                    urn, s_data, c_data = future.result()
                    prefetched_li_reg_stats[urn] = s_data
                    prefetched_li_reg_comments[urn] = c_data
                except Exception:
                    pass

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

        # Use pre-fetched engagement stats
        li_pre = prefetched_li_reg_stats.get(post_urn, {})
        likes_count = li_pre.get("likes", 0)
        comments_count_api = li_pre.get("comments_count", 0)

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

        # Use pre-fetched comments
        comments_data = prefetched_li_reg_comments.get(post_urn, [])

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

    _mark_owner_replies_for_page(page)
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
        resp = _http_session.get(
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

    # ── Pre-fetch ALL post stats + comments in parallel ──
    # LinkedIn requires a per-post API call for stats AND comments — parallelize both
    post_urns = [p.get("id", "") for p in elements if p.get("id")]
    prefetched_li_stats = {}
    prefetched_li_comments = {}

    def _fetch_li_stats_and_comments(urn):
        s = {"likes": 0, "comments": 0}
        comments = []
        try:
            sr = _http_session.get(f"{LI_BASE}/socialActions/{urn}", headers=headers, timeout=15)
            social = sr.json()
            s["likes"] = (social.get("likesSummary") or {}).get("totalLikes", 0)
            s["comments"] = (social.get("commentsSummary") or {}).get("totalFirstLevelComments", 0)
        except Exception:
            pass
        if s["comments"] > 0:
            try:
                cr = _http_session.get(
                    f"{LI_BASE}/socialActions/{urn}/comments",
                    headers=headers, params={"count": 100}, timeout=15,
                )
                if cr.status_code not in (403, 401):
                    comments = cr.json().get("elements", [])
            except Exception:
                pass
        return urn, s, comments

    if post_urns:
        with ThreadPoolExecutor(max_workers=min(8, len(post_urns))) as executor:
            futures = {executor.submit(_fetch_li_stats_and_comments, urn): urn for urn in post_urns}
            for future in as_completed(futures):
                try:
                    urn, s, comments = future.result()
                    prefetched_li_stats[urn] = s
                    prefetched_li_comments[urn] = comments
                except Exception:
                    pass

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

        # Use pre-fetched stats (already downloaded in parallel)
        li_stats = prefetched_li_stats.get(post_urn, {"likes": 0, "comments": 0})
        likes_count = li_stats["likes"]
        comments_count_api = li_stats["comments"]

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

        # Use pre-fetched comments (already downloaded in parallel)
        comments_data_li = prefetched_li_comments.get(post_urn, [])

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

    _mark_owner_replies_for_page(page)
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
            resp = _http_session.get(
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

    # ── Pre-fetch replies for tweets that need them (parallel) ──
    def _tw_reg_needs_replies(tw):
        tid = tw.get("id", "")
        rc = (tw.get("public_metrics") or {}).get("reply_count", 0)
        if rc == 0:
            return False
        ex = existing_posts.get(tid)
        if ex:
            return ex.comments_count != rc
        return True

    tw_reg_needing = [t for t in all_tweets if t.get("id") and _tw_reg_needs_replies(t)]
    prefetched_tw_reg = {}  # tweet_id -> {"replies": [...], "user_lookup": {...}}

    if tw_reg_needing:
        def _fetch_tw_reg_replies(tid, rc):
            tw_replies = []
            tw_users = {}
            try:
                r_resp = _http_session.get(
                    f"{TW_BASE}/tweets/search/recent",
                    headers=headers,
                    params={
                        "query": f"conversation_id:{tid} -from:{user_id}",
                        "max_results": min(100, max(rc + 10, 10)),
                        "tweet.fields": "created_at,author_id,text,public_metrics",
                        "expansions": "author_id",
                        "user.fields": "name,username",
                    },
                    timeout=15,
                )
                r_data = r_resp.json()
                tw_replies = r_data.get("data", [])
                for u in (r_data.get("includes") or {}).get("users", []):
                    tw_users[u["id"]] = u.get("name") or u.get("username") or "Unknown"
            except Exception:
                pass
            return tid, tw_replies, tw_users

        with ThreadPoolExecutor(max_workers=min(8, len(tw_reg_needing))) as executor:
            futures = {
                executor.submit(
                    _fetch_tw_reg_replies,
                    t.get("id", ""),
                    (t.get("public_metrics") or {}).get("reply_count", 0),
                ): t
                for t in tw_reg_needing
            }
            for future in as_completed(futures):
                try:
                    tid, r_list, u_map = future.result()
                    prefetched_tw_reg[tid] = {"replies": r_list, "user_lookup": u_map}
                except Exception:
                    pass

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

        # Use pre-fetched replies (already fetched in parallel above)
        tw_pre = prefetched_tw_reg.get(tweet_id, {})
        replies = tw_pre.get("replies", [])
        user_lookup = tw_pre.get("user_lookup", {})

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

    _mark_owner_replies_for_page(page)
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
            resp = _http_session.get(
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

    # ── Pre-fetch replies for all tweets that have them (parallel) ──
    def _tw_needs_replies(tweet):
        tid = tweet.get("id", "")
        rc = (tweet.get("public_metrics") or {}).get("reply_count", 0)
        ex = existing_posts.get(tid)
        if ex:
            return ex.comments_count != rc and rc > 0
        return rc > 0

    tweets_needing_replies = [t for t in all_tweets if _tw_needs_replies(t)]
    prefetched_tw_replies = {}

    def _fetch_tw_replies(tid, rc):
        replies = []
        users = {}
        try:
            r_resp = _http_session.get(
                f"{TW_BASE}/tweets/search/recent",
                headers=headers,
                params={
                    "query": f"conversation_id:{tid} -from:{user_id}",
                    "max_results": min(100, max(rc + 10, 10)),
                    "tweet.fields": "created_at,author_id,text,public_metrics",
                    "expansions": "author_id",
                    "user.fields": "name,username",
                },
                timeout=15,
            )
            r_data = r_resp.json()
            replies = r_data.get("data", [])
            for u in (r_data.get("includes") or {}).get("users", []):
                users[u["id"]] = u.get("name") or u.get("username") or "Unknown"
        except Exception:
            pass
        return tid, replies, users

    if tweets_needing_replies:
        with ThreadPoolExecutor(max_workers=min(8, len(tweets_needing_replies))) as executor:
            futures = {
                executor.submit(
                    _fetch_tw_replies,
                    t.get("id", ""),
                    (t.get("public_metrics") or {}).get("reply_count", 0),
                ): t for t in tweets_needing_replies
            }
            for future in as_completed(futures):
                try:
                    tid, reps, users = future.result()
                    prefetched_tw_replies[tid] = (reps, users)
                except Exception:
                    pass

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

        # Use pre-fetched replies (already downloaded in parallel)
        replies, user_lookup = prefetched_tw_replies.get(tweet_id, ([], {}))

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

    _mark_owner_replies_for_page(page)
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

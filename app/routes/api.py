import re
import json
from flask import Blueprint, jsonify, request, Response, stream_with_context, current_app
from flask_login import login_required, current_user
from app.extensions import db
from app.models.comment import Comment
from app.models.post import Post
from app.models.connected_page import ConnectedPage
from datetime import datetime, timezone

_DATE_LIKE_RE = re.compile(r"^\d{1,4}[-/]\d{1,2}[-/]\d{2,4}")


def _clean_phone(phone):
    """Return phone only if it looks like a real phone number, not a date."""
    if not phone:
        return None
    phone = phone.strip()
    if _DATE_LIKE_RE.match(phone):
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 7:
        return None
    return phone

api_bp = Blueprint("api", __name__, url_prefix="/api")


@api_bp.route("/check-email")
def check_email():
    """Check if an email is already registered (for real-time signup validation)."""
    from app.models.user import User
    email = request.args.get("email", "").strip().lower()
    if not email:
        return jsonify({"exists": False})
    exists = User.query.filter(db.func.lower(User.email) == email).first() is not None
    return jsonify({"exists": exists})


@api_bp.route("/dashboard/stats")
@login_required
def dashboard_stats():
    """Return dashboard chart data and KPI stats as JSON, with optional filters."""
    company_id = current_user.company_id
    if not company_id:
        return jsonify({"error": "No company assigned"}), 400

    # Read filter params
    platform = request.args.get("platform")
    page_id = request.args.get("page_id", type=int)
    post_id = request.args.get("post_id", type=int)

    # Build base queries with filters
    post_query = Post.query.filter_by(company_id=company_id)
    comment_query = Comment.query.filter_by(company_id=company_id, is_deleted=False)

    if platform:
        post_query = post_query.filter_by(platform=platform)
        comment_query = comment_query.filter_by(platform=platform)
    if page_id:
        post_query = post_query.filter_by(connected_page_id=page_id)
        comment_query = comment_query.filter(
            Comment.post_id.in_(
                db.session.query(Post.id).filter_by(connected_page_id=page_id)
            )
        )
    if post_id:
        post_query = post_query.filter_by(id=post_id)
        comment_query = comment_query.filter_by(post_id=post_id)

    # KPI stats
    total_posts = post_query.count()
    total_comments = comment_query.count()
    total_likes = db.session.query(
        db.func.coalesce(db.func.sum(Post.likes_count), 0)
    ).filter(
        Post.id.in_(post_query.with_entities(Post.id))
    ).scalar()
    total_shares = db.session.query(
        db.func.coalesce(db.func.sum(Post.shares_count), 0)
    ).filter(
        Post.id.in_(post_query.with_entities(Post.id))
    ).scalar()
    unreplied_comments = comment_query.filter(Comment.is_replied == False).count()

    # Contact count (filtered)
    from app.models.contact import Contact
    contact_query = Contact.query.filter_by(company_id=company_id)
    if platform:
        contact_query = contact_query.filter_by(platform=platform)
    if post_id:
        contact_query = contact_query.filter_by(source_post_id=post_id)
    total_contacts = contact_query.count()

    # Sentiment distribution
    sentiment_base = Comment.query.filter(
        Comment.company_id == company_id, Comment.is_deleted == False
    )
    if platform:
        sentiment_base = sentiment_base.filter_by(platform=platform)
    if page_id:
        sentiment_base = sentiment_base.filter(
            Comment.post_id.in_(
                db.session.query(Post.id).filter_by(connected_page_id=page_id)
            )
        )
    if post_id:
        sentiment_base = sentiment_base.filter_by(post_id=post_id)

    sentiments = (
        db.session.query(Comment.sentiment, db.func.count(Comment.id))
        .filter(Comment.id.in_(sentiment_base.with_entities(Comment.id)))
        .group_by(Comment.sentiment)
        .all()
    )

    # Platform post distribution
    platforms = (
        db.session.query(Post.platform, db.func.count(Post.id))
        .filter(Post.id.in_(post_query.with_entities(Post.id)))
        .group_by(Post.platform)
        .all()
    )

    # Comments over time (grouped by date)
    comments_timeline = (
        db.session.query(
            db.func.date(Comment.commented_at),
            db.func.count(Comment.id),
        )
        .filter(
            Comment.id.in_(comment_query.with_entities(Comment.id)),
            Comment.commented_at.isnot(None),
        )
        .group_by(db.func.date(Comment.commented_at))
        .order_by(db.func.date(Comment.commented_at))
        .all()
    )

    # Engagement breakdown by platform (likes, comments, shares, views)
    engagement_rows = (
        db.session.query(
            Post.platform,
            db.func.coalesce(db.func.sum(Post.likes_count), 0),
            db.func.coalesce(db.func.sum(Post.comments_count), 0),
            db.func.coalesce(db.func.sum(Post.shares_count), 0),
            db.func.coalesce(db.func.sum(Post.views), 0),
        )
        .filter(Post.id.in_(post_query.with_entities(Post.id)))
        .group_by(Post.platform)
        .all()
    )

    # Top 5 performing posts (by engagement = likes + comments + shares)
    top_posts = (
        post_query
        .order_by(
            (Post.likes_count + Post.comments_count + Post.shares_count).desc()
        )
        .limit(5)
        .all()
    )

    # Recent comments (filtered)
    recent_comments = (
        comment_query.order_by(Comment.commented_at.desc()).limit(5).all()
    )

    # Recent contacts (filtered)
    recent_contacts = (
        contact_query.order_by(Contact.created_at.desc()).limit(5).all()
    )

    return jsonify({
        "kpi": {
            "total_posts": total_posts,
            "total_comments": total_comments,
            "total_likes": total_likes,
            "total_shares": total_shares,
            "unreplied_comments": unreplied_comments,
            "total_contacts": total_contacts,
        },
        "sentiment": {s or "unknown": c for s, c in sentiments},
        "platforms": {p: c for p, c in platforms},
        "timeline": {str(d): c for d, c in comments_timeline} if comments_timeline else {},
        "engagement": {
            p: {"likes": int(l), "comments": int(c), "shares": int(s), "views": int(v)}
            for p, l, c, s, v in engagement_rows
        },
        "top_posts": [
            {
                "id": p.id,
                "caption": (p.caption or "(No caption)")[:60],
                "platform": p.platform,
                "likes": p.likes_count or 0,
                "comments": p.comments_count or 0,
                "shares": p.shares_count or 0,
                "views": p.views or 0,
                "thumbnail": p.thumbnail_url or p.media_url,
                "posted_at": p.posted_at.strftime("%b %d, %Y") if p.posted_at else "",
            }
            for p in top_posts
        ],
        "recent_comments": [
            {
                "author_name": c.display_name,
                "comment_text": c.comment_text[:80] if c.comment_text else "",
                "platform": c.platform,
                "sentiment": c.sentiment or "neutral",
                "sentiment_color": c.sentiment_color or "gray",
            }
            for c in recent_comments
        ],
        "recent_contacts": [
            {
                "name": ct.name or "Unknown",
                "email": ct.email,
                "phone": _clean_phone(ct.phone),
                "contact_type": ct.contact_type or "contact",
            }
            for ct in recent_contacts
        ],
    })


@api_bp.route("/dashboard/keywords")
@login_required
def dashboard_keywords():
    """Return top TF-IDF keywords extracted from all comments (filtered)."""
    company_id = current_user.company_id
    if not company_id:
        return jsonify({"error": "No company assigned"}), 400

    platform = request.args.get("platform")
    page_id = request.args.get("page_id", type=int)
    post_id = request.args.get("post_id", type=int)
    top_n = request.args.get("top_n", 30, type=int)

    query = Comment.query.filter_by(company_id=company_id, is_deleted=False)
    if platform:
        query = query.filter_by(platform=platform)
    if page_id:
        query = query.filter(
            Comment.post_id.in_(
                db.session.query(Post.id).filter_by(connected_page_id=page_id)
            )
        )
    if post_id:
        query = query.filter_by(post_id=post_id)

    comments = [
        c.comment_text
        for c in query.with_entities(Comment.comment_text).all()
        if c.comment_text
    ]

    from app.services.nlp_keywords import extract_top_keywords
    keywords = extract_top_keywords(comments, top_n=top_n)

    return jsonify({"keywords": keywords, "total_comments": len(comments)})


@api_bp.route("/pages/by-platform")
@login_required
def pages_by_platform():
    """Return connected pages filtered by platform (for cascading filters)."""
    company_id = current_user.company_id
    platform = request.args.get("platform")

    query = ConnectedPage.query.filter_by(company_id=company_id, status="connected")
    if platform:
        query = query.filter_by(platform=platform)

    pages = query.order_by(ConnectedPage.page_name).all()
    return jsonify({
        "pages": [
            {"id": p.id, "page_name": p.page_name, "platform": p.platform}
            for p in pages
        ]
    })


@api_bp.route("/posts/by-page")
@login_required
def posts_by_page():
    """Return posts filtered by connected page (for cascading filters)."""
    company_id = current_user.company_id
    page_id = request.args.get("page_id", type=int)
    platform = request.args.get("platform")

    query = Post.query.filter_by(company_id=company_id)
    if page_id:
        query = query.filter_by(connected_page_id=page_id)
    elif platform:
        query = query.filter_by(platform=platform)

    posts = query.order_by(Post.posted_at.desc()).all()
    return jsonify({
        "posts": [
            {
                "id": p.id,
                "caption": (p.caption or "(No caption)")[:80],
                "platform": p.platform,
                "media_type": p.media_type,
            }
            for p in posts
        ]
    })


@api_bp.route("/comments/<int:comment_id>/reply", methods=["POST"])
@login_required
def reply_comment(comment_id):
    comment = Comment.query.get_or_404(comment_id)
    if comment.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.get_json(silent=True) or {}
    reply_text = data.get("reply", "")

    # TODO: Actually post reply via platform API
    comment.is_replied = True
    comment.replied_by = current_user.id
    comment.replied_at = datetime.now(timezone.utc)
    comment.reply_text = reply_text
    db.session.commit()
    return jsonify({"success": True, "message": "Reply sent."})


@api_bp.route("/comments/<int:comment_id>/hide", methods=["POST"])
@login_required
def hide_comment(comment_id):
    comment = Comment.query.get_or_404(comment_id)
    if comment.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403

    comment.is_hidden = not comment.is_hidden
    db.session.commit()
    return jsonify({"success": True, "hidden": comment.is_hidden})


@api_bp.route("/comments/<int:comment_id>/flag", methods=["POST"])
@login_required
def flag_comment(comment_id):
    comment = Comment.query.get_or_404(comment_id)
    if comment.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403

    comment.is_flagged = not comment.is_flagged
    db.session.commit()
    return jsonify({"success": True, "flagged": comment.is_flagged})


@api_bp.route("/comments/<int:comment_id>/delete", methods=["DELETE"])
@login_required
def delete_comment(comment_id):
    comment = Comment.query.get_or_404(comment_id)
    if comment.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403

    comment.is_deleted = True
    db.session.commit()
    return jsonify({"success": True})


@api_bp.route("/languages")
@login_required
def supported_languages():
    """Return list of supported translation languages."""
    from app.services.ai_service import get_supported_languages
    langs = get_supported_languages()
    return jsonify({
        "languages": [
            {"code": code, "name": name.title()}
            for name, code in sorted(langs.items())
        ]
    })


@api_bp.route("/comments/<int:comment_id>/translate", methods=["POST"])
@login_required
def translate_comment(comment_id):
    """Translate a comment using Google Translate (deep-translator)."""
    comment = Comment.query.get_or_404(comment_id)
    if comment.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.get_json(silent=True) or {}
    target_lang = data.get("target_language", "en")
    source_lang = data.get("source_language", "auto")
    force = data.get("force", False)

    # Return cached translation only if same target+source language and not a failure
    cache_key = f"{target_lang}:{source_lang}"
    stored_key = f"{comment.target_language}:{comment.source_language or 'auto'}"
    if (not force
            and comment.translated_text
            and not comment.translated_text.startswith("[Translation")
            and cache_key == stored_key):
        return jsonify({
            "success": True,
            "translated_text": comment.translated_text,
            "detected_language": comment.detected_language,
        })

    from app.services.ai_service import translate_text
    result = translate_text(
        comment.comment_text,
        target_language=target_lang,
        source_language=source_lang,
    )
    translated = result["translated_text"]

    # Don't persist failed translations so they can be retried
    if translated.startswith("[Translation"):
        return jsonify({"error": "Translation failed. Please try again."}), 500

    comment.translated_text = translated
    comment.detected_language = result["detected_language"]
    comment.target_language = target_lang
    comment.source_language = source_lang
    db.session.commit()
    return jsonify({
        "success": True,
        "translated_text": comment.translated_text,
        "detected_language": comment.detected_language,
    })


@api_bp.route("/sync/<int:page_id>", methods=["POST"])
@login_required
def trigger_sync(page_id):
    page = ConnectedPage.query.get_or_404(page_id)
    if page.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403

    from app.services.sync_engine import sync_page
    result = sync_page(page_id)

    if "error" in result:
        return jsonify({"success": False, "error": result["error"]})

    posts = result.get("posts_synced", 0)
    comments = result.get("comments_synced", 0)
    contacts = result.get("contacts_found", 0)
    return jsonify({
        "success": True,
        "message": f"Synced {posts} posts, {comments} comments, {contacts} contacts for {page.page_name}.",
    })


@api_bp.route("/sync/<int:page_id>/search", methods=["POST"])
@login_required
def sync_search_videos(page_id):
    """Search a YouTube channel's videos by keyword for pre-sync selection."""
    page = ConnectedPage.query.get_or_404(page_id)
    if page.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403
    if page.platform != "youtube":
        return jsonify({"error": "Keyword search is only supported for YouTube pages."}), 400

    data = request.get_json(silent=True) or {}
    keyword = (data.get("keyword") or "").strip()
    limit = min(int(data.get("limit", 50)), 50)

    if not keyword:
        return jsonify({"error": "A keyword is required."}), 400

    from app.services.youtube_service import YouTubeService
    service = YouTubeService(current_user.company_id)
    if not service.is_configured:
        return jsonify({"error": "YouTube API key not configured."}), 400

    items = service.fetch_channel_videos(page.page_id, limit=limit, keyword=keyword)

    videos = []
    for v in items:
        vid_id = v.get("id", {}).get("videoId") if isinstance(v.get("id"), dict) else v.get("id")
        if not vid_id:
            continue
        snippet = v.get("snippet", {})
        thumbnails = snippet.get("thumbnails") or {}
        thumbnail = (
            thumbnails.get("high", {}).get("url")
            or thumbnails.get("medium", {}).get("url")
            or thumbnails.get("default", {}).get("url")
        )
        videos.append({
            "video_id": vid_id,
            "title": snippet.get("title", "Untitled"),
            "thumbnail": thumbnail,
            "published_at": snippet.get("publishedAt", ""),
            "channel_title": snippet.get("channelTitle", ""),
            "_raw": v,
        })

    return jsonify({"videos": videos, "total": len(videos), "keyword": keyword})


@api_bp.route("/sync/<int:page_id>/stream", methods=["POST"])
@login_required
def sync_stream(page_id):
    """Stream sync progress as Server-Sent Events (YouTube channels)."""
    page = ConnectedPage.query.get_or_404(page_id)
    if page.company_id != current_user.company_id:
        return jsonify({"error": "Unauthorized"}), 403

    # Read optional video selection from JSON body
    data = request.get_json(silent=True) or {}
    selected_videos = data.get("video_objects")

    sse_headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }

    from app.services.sync_engine import (
        sync_youtube_stream,
        sync_facebook_stream,
        sync_instagram_stream,
        sync_linkedin_stream,
        sync_twitter_stream,
    )

    STREAM_MAP = {
        "youtube": lambda: sync_youtube_stream(page, video_objects=selected_videos),
        "facebook": lambda: sync_facebook_stream(page),
        "instagram": lambda: sync_instagram_stream(page),
        "linkedin": lambda: sync_linkedin_stream(page),
        "twitter": lambda: sync_twitter_stream(page),
    }

    stream_fn = STREAM_MAP.get(page.platform)

    if stream_fn is None:
        from app.services.sync_engine import sync_page
        result = sync_page(page_id)

        def single_event():
            yield ": ping\n\n"
            if "error" in result:
                yield f"data: {json.dumps({'type': 'error', 'error': result['error']})}\n\n"
            else:
                yield f"data: {json.dumps({'type': 'done', **result})}\n\n"

        return Response(
            stream_with_context(single_event()),
            content_type="text/event-stream",
            headers=sse_headers,
        )

    def generate():
        yield ": ping\n\n"
        for event in stream_fn():
            yield f"data: {json.dumps(event)}\n\n"

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers=sse_headers,
    )


@api_bp.route("/comments")
@login_required
def get_comments():
    """AJAX endpoint for comment filtering."""
    company_id = current_user.company_id
    query = Comment.query.filter_by(company_id=company_id, is_deleted=False)

    platform = request.args.get("platform")
    if platform:
        query = query.filter_by(platform=platform)

    sentiment = request.args.get("sentiment")
    if sentiment:
        query = query.filter_by(sentiment=sentiment)

    post_id = request.args.get("post_id", type=int)
    if post_id:
        query = query.filter_by(post_id=post_id)

    search = request.args.get("q")
    if search:
        query = query.filter(Comment.comment_text.ilike(f"%{search}%"))

    page = request.args.get("page", 1, type=int)
    pagination = query.order_by(Comment.commented_at.desc()).paginate(page=page, per_page=20, error_out=False)

    comments = [
        {
            "id": c.id,
            "author_name": c.author_name,
            "comment_text": c.comment_text,
            "platform": c.platform,
            "sentiment": c.sentiment,
            "sentiment_color": c.sentiment_color,
            "has_contact_info": c.has_contact_info,
            "is_replied": c.is_replied,
            "is_flagged": c.is_flagged,
            "commented_at": c.commented_at.strftime("%Y-%m-%d %H:%M") if c.commented_at else "",
        }
        for c in pagination.items
    ]

    return jsonify({
        "comments": comments,
        "total": pagination.total,
        "pages": pagination.pages,
        "current_page": pagination.page,
    })


# ── Author Profile API ──────────────────────────────────────

@api_bp.route("/author/<path:author_name>/sentiment")
@login_required
def author_sentiment(author_name):
    """Return sentiment, timeline, and platform data for an author's Chart.js charts."""
    from app.routes.comments import _author_name_filter

    company_id = current_user.company_id
    base_filter = [
        Comment.company_id == company_id,
        Comment.is_deleted == False,
        _author_name_filter(author_name),
    ]

    # Sentiment distribution
    sentiments = (
        db.session.query(Comment.sentiment, db.func.count(Comment.id))
        .filter(*base_filter)
        .group_by(Comment.sentiment)
        .all()
    )

    # Comments over time (grouped by date)
    timeline = (
        db.session.query(
            db.func.date(Comment.commented_at),
            db.func.count(Comment.id),
        )
        .filter(*base_filter, Comment.commented_at.isnot(None))
        .group_by(db.func.date(Comment.commented_at))
        .order_by(db.func.date(Comment.commented_at))
        .all()
    )

    # Platform distribution
    platforms = (
        db.session.query(Comment.platform, db.func.count(Comment.id))
        .filter(*base_filter)
        .group_by(Comment.platform)
        .all()
    )

    return jsonify({
        "sentiment": {s or "unknown": c for s, c in sentiments},
        "timeline": {str(d): c for d, c in timeline} if timeline else {},
        "platforms": {p: c for p, c in platforms},
    })


@api_bp.route("/author/<path:author_name>/reanalyze", methods=["POST"])
@login_required
def author_reanalyze(author_name):
    """Re-analyze all comments by this author using AI sentiment analysis."""
    from app.routes.comments import _author_name_filter
    from app.services.ai_service import analyze_sentiment

    company_id = current_user.company_id
    comments = Comment.query.filter(
        Comment.company_id == company_id,
        Comment.is_deleted == False,
        _author_name_filter(author_name),
    ).all()

    updated = 0
    errors = 0
    for comment in comments:
        try:
            result = analyze_sentiment(comment.comment_text)
            comment.sentiment = result.get("sentiment", comment.sentiment)
            comment.sentiment_score = result.get("score", comment.sentiment_score)
            updated += 1
        except Exception:
            errors += 1

    db.session.commit()

    return jsonify({
        "success": True,
        "updated": updated,
        "errors": errors,
        "total": len(comments),
        "message": f"Re-analyzed {updated} of {len(comments)} comments.",
    })


@api_bp.route("/comments/reanalyze-all", methods=["POST"])
@login_required
def reanalyze_all_comments():
    """Re-analyze sentiment for all neutral comments using improved heuristic.

    Uses SSE streaming to send progress updates so the request doesn't timeout.
    Skips Google Translate for speed (uses keyword + emoji matching).
    """
    from app.services.ai_service import _heuristic_sentiment

    company_id = current_user.company_id
    comment_ids = [
        c.id for c in db.session.query(Comment.id).filter(
            Comment.company_id == company_id,
            Comment.is_deleted == False,
            Comment.sentiment == "neutral",
        ).all()
    ]
    total = len(comment_ids)

    def generate():
        import json as _json
        yield f"data: {_json.dumps({'type': 'start', 'total': total})}\n\n"

        updated = 0
        batch_size = 200

        for i in range(0, total, batch_size):
            batch_ids = comment_ids[i : i + batch_size]
            batch = Comment.query.filter(Comment.id.in_(batch_ids)).all()

            for comment in batch:
                try:
                    result = _heuristic_sentiment(
                        comment.comment_text, skip_translate=True
                    )
                    new_sentiment = result.get("sentiment", "neutral")
                    if new_sentiment != "neutral":
                        comment.sentiment = new_sentiment
                        comment.sentiment_score = result.get("score", 0.5)
                        updated += 1
                except Exception:
                    pass

            db.session.commit()

            processed = min(i + batch_size, total)
            pct = round((processed / total) * 100) if total else 100
            yield f"data: {_json.dumps({'type': 'progress', 'processed': processed, 'total': total, 'updated': updated, 'percent': pct})}\n\n"

        yield f"data: {_json.dumps({'type': 'done', 'updated': updated, 'total': total})}\n\n"

    return current_app.response_class(
        stream_with_context(generate()), mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

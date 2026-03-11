import re
import json
from flask import Blueprint, render_template
from flask_login import login_required, current_user
from app.extensions import db
from app.models.post import Post
from app.models.comment import Comment
from app.models.connected_page import ConnectedPage
from app.models.contact import Contact
from app.utils.decorators import permission_required
from app.utils.constants import PLATFORMS, PLATFORM_LABELS

_DATE_LIKE_RE = re.compile(r"^\d{1,4}[-/]\d{1,2}[-/]\d{2,4}")

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/dashboard")


def _clean_phone(phone):
    if not phone:
        return None
    phone = phone.strip()
    if _DATE_LIKE_RE.match(phone):
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 7:
        return None
    return phone


def _detect_owner_replies(company_id):
    """Mark comments as replied when the page owner has responded on the platform."""
    pages = ConnectedPage.query.filter_by(company_id=company_id, status="connected").all()
    for page in pages:
        if not page.page_id:
            continue
        # Find posts where the page owner has commented
        owner_post_ids = (
            db.session.query(Comment.post_id)
            .filter(
                Comment.company_id == company_id,
                Comment.platform_author_id == page.page_id,
            )
            .distinct()
            .subquery()
        )
        # Mark non-owner comments on those posts as replied
        Comment.query.filter(
            Comment.post_id.in_(db.session.query(owner_post_ids.c.post_id)),
            Comment.platform_author_id != page.page_id,
            Comment.is_replied == False,
        ).update({"is_replied": True}, synchronize_session="fetch")
    db.session.commit()


def _build_dashboard_data(company_id):
    """Build the full dashboard JSON payload (same structure as /api/dashboard/stats)."""
    post_query = Post.query.filter_by(company_id=company_id)
    comment_query = Comment.query.filter_by(company_id=company_id, is_deleted=False)

    total_posts = post_query.count()
    total_comments = comment_query.count()
    total_likes = db.session.query(
        db.func.coalesce(db.func.sum(Post.likes_count), 0)
    ).filter_by(company_id=company_id).scalar()
    total_shares = db.session.query(
        db.func.coalesce(db.func.sum(Post.shares_count), 0)
    ).filter_by(company_id=company_id).scalar()
    unreplied_comments = comment_query.filter(Comment.is_replied == False).count()
    total_contacts = Contact.query.filter_by(company_id=company_id).count()

    # Sentiment
    sentiments = (
        db.session.query(Comment.sentiment, db.func.count(Comment.id))
        .filter(Comment.company_id == company_id, Comment.is_deleted == False)
        .group_by(Comment.sentiment)
        .all()
    )

    # Platform posts
    platforms = (
        db.session.query(Post.platform, db.func.count(Post.id))
        .filter_by(company_id=company_id)
        .group_by(Post.platform)
        .all()
    )

    # Timeline
    comments_timeline = (
        db.session.query(
            db.func.date(Comment.commented_at),
            db.func.count(Comment.id),
        )
        .filter(
            Comment.company_id == company_id,
            Comment.is_deleted == False,
            Comment.commented_at.isnot(None),
        )
        .group_by(db.func.date(Comment.commented_at))
        .order_by(db.func.date(Comment.commented_at))
        .all()
    )

    # Actual comment counts by platform (from Comment table, matches KPI)
    comment_platform_rows = (
        db.session.query(Comment.platform, db.func.count(Comment.id))
        .filter(Comment.company_id == company_id, Comment.is_deleted == False)
        .group_by(Comment.platform)
        .all()
    )

    # Engagement by platform
    engagement_rows = (
        db.session.query(
            Post.platform,
            db.func.coalesce(db.func.sum(Post.likes_count), 0),
            db.func.coalesce(db.func.sum(Post.comments_count), 0),
            db.func.coalesce(db.func.sum(Post.shares_count), 0),
            db.func.coalesce(db.func.sum(Post.views), 0),
        )
        .filter_by(company_id=company_id)
        .group_by(Post.platform)
        .all()
    )

    # Top posts
    top_posts = (
        post_query
        .order_by((Post.likes_count + Post.comments_count + Post.shares_count).desc())
        .limit(5)
        .all()
    )

    # Recent comments
    recent_comments = (
        comment_query.order_by(Comment.commented_at.desc()).limit(5).all()
    )

    # Recent contacts
    recent_contacts = (
        Contact.query.filter_by(company_id=company_id)
        .order_by(Contact.created_at.desc())
        .limit(5)
        .all()
    )

    return {
        "kpi": {
            "total_posts": total_posts,
            "total_comments": total_comments,
            "total_likes": int(total_likes),
            "total_shares": int(total_shares),
            "unreplied_comments": unreplied_comments,
            "total_contacts": total_contacts,
        },
        "comment_platforms": {p: c for p, c in comment_platform_rows},
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
    }


@dashboard_bp.route("/")
@login_required
@permission_required("dashboard")
def index():
    company_id = current_user.company_id

    # Detect page-owner replies so the Response Tracker reflects platform replies
    _detect_owner_replies(company_id)

    # Build full chart data for instant rendering (no API call needed on first load)
    initial_data = _build_dashboard_data(company_id)

    stats = {
        "total_pages": ConnectedPage.query.filter_by(company_id=company_id, status="connected").count(),
        "total_posts": initial_data["kpi"]["total_posts"],
        "total_comments": initial_data["kpi"]["total_comments"],
        "total_contacts": initial_data["kpi"]["total_contacts"],
        "total_likes": initial_data["kpi"]["total_likes"],
        "total_shares": initial_data["kpi"]["total_shares"],
        "total_views": sum(e["views"] for e in initial_data["engagement"].values()),
        "unreplied_comments": initial_data["kpi"]["unreplied_comments"],
    }

    # Recent comments & contacts for server-rendered sections
    recent_comments = (
        Comment.query.filter_by(company_id=company_id, is_deleted=False)
        .order_by(Comment.commented_at.desc())
        .limit(5)
        .all()
    )
    recent_contacts = (
        Contact.query.filter_by(company_id=company_id)
        .order_by(Contact.created_at.desc())
        .limit(5)
        .all()
    )

    connected_platforms = (
        db.session.query(ConnectedPage.platform)
        .filter_by(company_id=company_id, status="connected")
        .distinct()
        .all()
    )
    connected_platforms = [p[0] for p in connected_platforms]

    # Pre-compute keywords server-side for instant rendering
    from app.services.nlp_keywords import extract_top_keywords
    all_comment_texts = [
        c.comment_text
        for c in Comment.query.filter_by(company_id=company_id, is_deleted=False)
        .with_entities(Comment.comment_text).all()
        if c.comment_text
    ]
    initial_data["keywords"] = extract_top_keywords(all_comment_texts, top_n=30)
    initial_data["keywords_total"] = len(all_comment_texts)

    return render_template(
        "dashboard/index.html",
        stats=stats,
        recent_comments=recent_comments,
        recent_contacts=recent_contacts,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
        connected_platforms=connected_platforms,
        initial_data_json=json.dumps(initial_data),
    )

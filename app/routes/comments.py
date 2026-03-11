from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload
from app.extensions import db
from app.models.comment import Comment
from app.models.post import Post
from app.models.post_reaction import PostReaction
from app.models.connected_page import ConnectedPage
from app.utils.helpers import get_pagination_args, export_csv
from app.utils.decorators import permission_required
from app.utils.constants import PLATFORMS, PLATFORM_LABELS, SENTIMENT_TYPES


def _author_name_filter(author_name):
    """Build a SQLAlchemy filter that matches by author_name or platform_author_id."""
    name_condition = func.lower(Comment.author_name) == author_name.lower()
    if author_name.startswith("User "):
        potential_id = author_name[5:]
        name_condition = or_(
            name_condition,
            Comment.platform_author_id == potential_id,
        )
    return name_condition

comments_bp = Blueprint("comments", __name__, url_prefix="/comments")


@comments_bp.route("/")
@login_required
@permission_required("comments")
def index():
    page, per_page = get_pagination_args()
    company_id = current_user.company_id

    query = Comment.query.options(joinedload(Comment.post)).filter_by(company_id=company_id, is_deleted=False)

    # Filters
    platform = request.args.get("platform")
    if platform:
        query = query.filter_by(platform=platform)

    page_id = request.args.get("page_id", type=int)
    if page_id:
        query = query.filter(
            Comment.post_id.in_(
                db.session.query(Post.id).filter_by(connected_page_id=page_id)
            )
        )

    sentiment = request.args.get("sentiment")
    if sentiment:
        query = query.filter_by(sentiment=sentiment)

    post_id = request.args.get("post_id", type=int)
    if post_id:
        query = query.filter_by(post_id=post_id)

    has_contact = request.args.get("has_contact")
    if has_contact == "1":
        query = query.filter_by(has_contact_info=True)

    search = request.args.get("q")
    if search:
        query = query.filter(Comment.comment_text.ilike(f"%{search}%"))

    query = query.order_by(Comment.commented_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    # Get all posts for the post filter dropdown (filtered by page if selected)
    post_query = Post.query.filter_by(company_id=company_id)
    if page_id:
        post_query = post_query.filter_by(connected_page_id=page_id)
    posts = post_query.order_by(Post.posted_at.desc()).all()

    # Connected pages for filter dropdown
    connected_pages = ConnectedPage.query.filter_by(
        company_id=company_id, status="connected"
    ).order_by(ConnectedPage.page_name).all()

    # Handle AJAX requests
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return render_template(
            "comments/_comment_list.html",
            comments=pagination.items,
            pagination=pagination,
        )

    return render_template(
        "comments/index.html",
        comments=pagination.items,
        pagination=pagination,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
        sentiment_types=SENTIMENT_TYPES,
        posts=posts,
        connected_pages=connected_pages,
    )


@comments_bp.route("/export")
@login_required
@permission_required("comments", "view")
def export():
    company_id = current_user.company_id
    comments = Comment.query.filter_by(company_id=company_id, is_deleted=False).order_by(Comment.commented_at.desc()).all()

    headers = ["ID", "Author", "Comment", "Platform", "Sentiment", "Has Contact", "Date"]
    rows = [
        [
            c.id,
            c.display_name,
            c.comment_text,
            c.platform,
            c.sentiment or "",
            "Yes" if c.has_contact_info else "No",
            c.commented_at.strftime("%Y-%m-%d %H:%M") if c.commented_at else "",
        ]
        for c in comments
    ]

    return export_csv(rows, headers, filename="comments_export.csv")


@comments_bp.route("/users")
@login_required
@permission_required("comments")
def user_monitor():
    """List all unique comment authors with stats."""
    company_id = current_user.company_id
    page, per_page = get_pagination_args(default_per_page=20)
    search = request.args.get("q", "").strip()
    platform_filter = request.args.get("platform")
    page_id = request.args.get("page_id", type=int)

    # Get unique authors with aggregated stats
    base_filter = [
        Comment.company_id == company_id,
        Comment.is_deleted == False,
        Comment.author_name.isnot(None),
        Comment.author_name != "",
        Comment.author_name != "Unknown",
        func.trim(Comment.author_name) != "",
    ]

    if platform_filter:
        base_filter.append(Comment.platform == platform_filter)

    if page_id:
        base_filter.append(
            Comment.post_id.in_(
                db.session.query(Post.id).filter_by(connected_page_id=page_id)
            )
        )

    if search:
        base_filter.append(
            or_(
                Comment.author_name.ilike(f"%{search}%"),
                Comment.platform_author_id.ilike(f"%{search}%"),
            )
        )

    # Query unique authors with comment count and latest comment date
    author_query = (
        db.session.query(
            Comment.author_name,
            Comment.platform_author_id,
            func.max(Comment.platform_author_id).label("platform_author_id"),
            func.count(Comment.id).label("comment_count"),
            func.max(Comment.commented_at).label("last_comment"),
            func.group_concat(Comment.platform.distinct()).label("platforms"),
        )
        .filter(*base_filter)
        .group_by(Comment.author_name)
        .order_by(func.count(Comment.id).desc())
    )

    # Manual pagination
    total = author_query.count()
    authors = author_query.offset((page - 1) * per_page).limit(per_page).all()

    # Build per-platform author ID mapping for profile links
    author_names = [a.author_name for a in authors]
    author_platform_ids = {}
    if author_names:
        pid_rows = (
            db.session.query(
                Comment.author_name, Comment.platform, Comment.platform_author_id
            )
            .filter(
                Comment.company_id == company_id,
                Comment.is_deleted == False,
                Comment.author_name.in_(author_names),
                Comment.platform_author_id.isnot(None),
            )
            .distinct()
            .all()
        )
        for name, plat, pid in pid_rows:
            author_platform_ids.setdefault(name, {})[plat] = pid

    # Connected pages for filter dropdown
    connected_pages = ConnectedPage.query.filter_by(
        company_id=company_id, status="connected"
    ).order_by(ConnectedPage.page_name).all()

    # Build simple pagination object
    from math import ceil

    class SimplePagination:
        def __init__(self, items, total, page, per_page):
            self.items = items
            self.total = total
            self.page = page
            self.per_page = per_page
            self.pages = ceil(total / per_page) if per_page else 1
            self.has_prev = page > 1
            self.has_next = page < self.pages
            self.prev_num = page - 1 if self.has_prev else None
            self.next_num = page + 1 if self.has_next else None

        def iter_pages(self, left_edge=2, left_current=2, right_current=3, right_edge=2):
            last = 0
            for num in range(1, self.pages + 1):
                if (
                    num <= left_edge
                    or (self.page - left_current <= num <= self.page + right_current)
                    or num > self.pages - right_edge
                ):
                    if last + 1 != num:
                        yield None
                    yield num
                    last = num

    pagination = SimplePagination(authors, total, page, per_page)

    return render_template(
        "comments/user_monitor.html",
        authors=authors,
        pagination=pagination,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
        connected_pages=connected_pages,
        author_platform_ids=author_platform_ids,
    )


@comments_bp.route("/author/<path:author_name>")
@login_required
@permission_required("comments")
def author_profile(author_name):
    """Show author profile with all their comments and sentiment breakdown."""
    company_id = current_user.company_id
    page, per_page = get_pagination_args()

    # Base query: all comments by this author (matches author_name or platform_author_id)
    base_filter = [
        Comment.company_id == company_id,
        Comment.is_deleted == False,
        _author_name_filter(author_name),
    ]

    # Aggregate stats via DB queries (fast, no full load)
    total_comments = Comment.query.filter(*base_filter).count()

    sentiment_rows = (
        db.session.query(Comment.sentiment, func.count(Comment.id))
        .filter(*base_filter)
        .group_by(Comment.sentiment)
        .all()
    )
    sentiment_counts = {s: 0 for s in SENTIMENT_TYPES}
    for s, cnt in sentiment_rows:
        if s in sentiment_counts:
            sentiment_counts[s] = cnt

    # Unique posts and platforms
    post_ids = [
        r[0]
        for r in db.session.query(Comment.post_id)
        .filter(*base_filter)
        .distinct()
        .all()
    ]
    platforms_active = [
        r[0]
        for r in db.session.query(Comment.platform)
        .filter(*base_filter)
        .distinct()
        .all()
    ]

    # Date range
    date_range = db.session.query(
        func.min(Comment.commented_at), func.max(Comment.commented_at)
    ).filter(*base_filter).first()
    first_comment = date_range[0] if date_range else None
    last_comment = date_range[1] if date_range else None

    has_contact = (
        Comment.query.filter(*base_filter, Comment.has_contact_info == True).first()
        is not None
    )

    # Get the platform author ID for display (from comments or reactions)
    author_id_row = (
        db.session.query(Comment.platform_author_id)
        .filter(*base_filter, Comment.platform_author_id.isnot(None))
        .first()
    )
    platform_author_id = author_id_row[0] if author_id_row else None

    posts_commented = (
        Post.query.filter(Post.id.in_(post_ids)).order_by(Post.posted_at.desc()).all()
        if post_ids
        else []
    )

    # Build mapping: post_id → list of comments by this author on that post
    comments_by_post = {}
    if post_ids:
        author_comments_all = (
            Comment.query.filter(*base_filter)
            .order_by(Comment.commented_at.desc())
            .all()
        )
        for c in author_comments_all:
            comments_by_post.setdefault(c.post_id, []).append(c)

    # Paginated comments for display
    pagination = (
        Comment.query.filter(*base_filter)
        .order_by(Comment.commented_at.desc())
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    # --- Likes stats for this user (from PostReaction table) ---
    reaction_filter = [
        PostReaction.company_id == company_id,
        func.lower(PostReaction.user_name) == author_name.lower(),
    ]
    total_likes_given = PostReaction.query.filter(*reaction_filter).count()

    reacted_post_ids = [
        r[0]
        for r in db.session.query(PostReaction.post_id)
        .filter(*reaction_filter)
        .distinct()
        .all()
    ]
    posts_reacted = (
        Post.query.filter(Post.id.in_(reacted_post_ids)).order_by(Post.posted_at.desc()).all()
        if reacted_post_ids
        else []
    )

    # Fall back to PostReaction for author ID if not found in comments
    if not platform_author_id:
        reaction_id_row = (
            db.session.query(PostReaction.platform_user_id)
            .filter(*reaction_filter, PostReaction.platform_user_id.isnot(None))
            .first()
        )
        platform_author_id = reaction_id_row[0] if reaction_id_row else None

    # Combine platforms from both comments and reactions
    reaction_platforms = [
        r[0]
        for r in db.session.query(PostReaction.platform)
        .filter(*reaction_filter)
        .distinct()
        .all()
    ]
    all_platforms = list(set(platforms_active + reaction_platforms))

    # Combined unique posts count
    all_post_ids = list(set(post_ids + reacted_post_ids))

    # Per-platform author IDs for profile links
    platform_id_rows = (
        db.session.query(Comment.platform, Comment.platform_author_id)
        .filter(*base_filter, Comment.platform_author_id.isnot(None))
        .distinct()
        .all()
    )
    author_platform_ids = {plat: pid for plat, pid in platform_id_rows}

    stats = {
        "total_comments": total_comments,
        "total_likes": total_likes_given,
        "sentiment_counts": sentiment_counts,
        "posts_count": len(all_post_ids),
        "platforms_active": all_platforms,
        "first_comment": first_comment,
        "last_comment": last_comment,
        "has_contact_info": has_contact,
    }

    return render_template(
        "comments/author.html",
        author_name=author_name,
        platform_author_id=platform_author_id,
        author_platform_ids=author_platform_ids,
        comments=pagination.items,
        pagination=pagination,
        stats=stats,
        posts_commented=posts_commented,
        comments_by_post=comments_by_post,
        posts_reacted=posts_reacted,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
        sentiment_types=SENTIMENT_TYPES,
    )

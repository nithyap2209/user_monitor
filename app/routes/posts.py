from flask import Blueprint, render_template, request, abort
from flask_login import login_required, current_user
from app.extensions import db
from app.models.post import Post
from app.models.comment import Comment
from app.models.post_reaction import PostReaction
from app.models.connected_page import ConnectedPage
from app.utils.helpers import get_pagination_args
from app.utils.decorators import permission_required
from app.utils.constants import PLATFORMS, PLATFORM_LABELS, SENTIMENT_TYPES

posts_bp = Blueprint("posts", __name__, url_prefix="/posts")


@posts_bp.route("/")
@login_required
@permission_required("posts")
def index():
    page, per_page = get_pagination_args()
    company_id = current_user.company_id

    query = Post.query.filter_by(company_id=company_id)

    # Filters
    platform = request.args.get("platform")
    if platform:
        query = query.filter_by(platform=platform)

    page_id = request.args.get("page_id", type=int)
    if page_id:
        query = query.filter_by(connected_page_id=page_id)

    search = request.args.get("q")
    if search:
        query = query.filter(Post.caption.ilike(f"%{search}%"))

    sort = request.args.get("sort", "date")
    if sort == "likes":
        query = query.order_by(Post.likes_count.desc())
    elif sort == "comments":
        query = query.order_by(Post.comments_count.desc())
    elif sort == "reach":
        query = query.order_by(Post.reach.desc())
    else:
        query = query.order_by(Post.posted_at.desc())

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    # Connected pages for filter dropdown
    connected_pages = ConnectedPage.query.filter_by(
        company_id=company_id, status="connected"
    ).order_by(ConnectedPage.page_name).all()

    return render_template(
        "posts/index.html",
        posts=pagination.items,
        pagination=pagination,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
        connected_pages=connected_pages,
    )


@posts_bp.route("/<int:post_id>")
@login_required
@permission_required("posts")
def detail(post_id):
    post = Post.query.get_or_404(post_id)
    if post.company_id != current_user.company_id:
        abort(403)

    comments = post.comments.filter_by(is_deleted=False).order_by(Comment.commented_at.asc()).all()

    # Post-level stats
    post_stats = {
        "total_comments": len(comments),
        "positive": sum(1 for c in comments if c.sentiment == "positive"),
        "negative": sum(1 for c in comments if c.sentiment == "negative"),
        "neutral": sum(1 for c in comments if c.sentiment == "neutral"),
        "leads": sum(1 for c in comments if c.sentiment == "lead"),
    }

    # Fetch reactions for this post
    reactions = PostReaction.query.filter_by(post_id=post.id).order_by(PostReaction.reaction_type).all()
    reaction_summary = {}
    for r in reactions:
        rtype = r.reaction_type or "LIKE"
        reaction_summary[rtype] = reaction_summary.get(rtype, 0) + 1

    # Page owner activity: find the page owner's likes and comments
    page_owner_reaction = None
    page_owner_comments = []
    page_id = post.connected_page.page_id if post.connected_page else None
    page_name = post.connected_page.page_name if post.connected_page else None
    if page_id:
        page_owner_reaction = next(
            (r for r in reactions if r.platform_user_id == page_id), None
        )
        page_owner_comments = [
            c for c in comments if c.platform_author_id == page_id
        ]

    # Unique commenters for engagement display
    seen_names = set()
    unique_commenters = []
    for c in comments:
        name = c.display_name
        if name and name != "Unknown" and name not in seen_names:
            seen_names.add(name)
            unique_commenters.append({
                "name": name,
                "count": sum(1 for x in comments if x.display_name == name),
            })

    return render_template(
        "posts/detail.html",
        post=post,
        comments=comments,
        post_stats=post_stats,
        sentiment_types=SENTIMENT_TYPES,
        reactions=reactions,
        reaction_summary=reaction_summary,
        page_owner_reaction=page_owner_reaction,
        page_owner_comments=page_owner_comments,
        page_name=page_name,
        unique_commenters=unique_commenters,
    )

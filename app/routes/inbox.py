from flask import Blueprint, render_template, request
from flask_login import login_required, current_user
from app.models.comment import Comment
from app.models.post import Post
from app.extensions import db
from app.utils.helpers import get_pagination_args
from app.utils.decorators import permission_required
from app.utils.constants import PLATFORMS, PLATFORM_LABELS, SENTIMENT_TYPES

inbox_bp = Blueprint("inbox", __name__, url_prefix="/inbox")


@inbox_bp.route("/")
@login_required
@permission_required("comments")
def index():
    page, per_page = get_pagination_args()
    company_id = current_user.company_id

    filter_type = request.args.get("filter", "unreplied")
    selected_platform = request.args.get("platform", "")
    selected_page_id = request.args.get("page_id", "", type=str)
    selected_post_id = request.args.get("post_id", "", type=str)

    if filter_type == "flagged":
        query = Comment.query.filter_by(company_id=company_id, is_flagged=True, is_deleted=False)
    elif filter_type == "negative":
        query = Comment.query.filter_by(company_id=company_id, sentiment="negative", is_deleted=False)
    elif filter_type == "leads":
        query = Comment.query.filter_by(company_id=company_id, sentiment="lead", is_deleted=False)
    elif filter_type == "all":
        query = Comment.query.filter_by(company_id=company_id, is_deleted=False)
    else:  # unreplied
        query = Comment.query.filter_by(company_id=company_id, is_replied=False, is_hidden=False, is_deleted=False)

    # Apply platform filter
    if selected_platform:
        query = query.filter(Comment.platform == selected_platform)

    # Apply page/channel filter
    if selected_page_id:
        query = query.filter(
            Comment.post_id.in_(
                db.session.query(Post.id).filter_by(connected_page_id=int(selected_page_id))
            )
        )

    # Apply post filter
    if selected_post_id:
        query = query.filter(Comment.post_id == int(selected_post_id))

    query = query.order_by(Comment.commented_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    # Active comment for split panel (first one or selected)
    active_comment_id = request.args.get("active", type=int)
    explicit_active = bool(active_comment_id)  # True only if user explicitly selected
    active_comment = None
    if active_comment_id:
        active_comment = Comment.query.get(active_comment_id)
    elif pagination.items:
        active_comment = pagination.items[0]  # auto-select for desktop only

    return render_template(
        "inbox/index.html",
        comments=pagination.items,
        pagination=pagination,
        filter_type=filter_type,
        active_comment=active_comment,
        explicit_active=explicit_active,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
        selected_platform=selected_platform,
        selected_page_id=selected_page_id,
        selected_post_id=selected_post_id,
    )

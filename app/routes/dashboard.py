from flask import Blueprint, render_template
from flask_login import login_required, current_user
from app.extensions import db
from app.models.post import Post
from app.models.comment import Comment
from app.models.connected_page import ConnectedPage
from app.models.contact import Contact
from app.utils.decorators import permission_required
from app.utils.constants import PLATFORMS, PLATFORM_LABELS

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/dashboard")


@dashboard_bp.route("/")
@login_required
@permission_required("dashboard")
def index():
    company_id = current_user.company_id

    stats = {
        "total_pages": ConnectedPage.query.filter_by(company_id=company_id, status="connected").count(),
        "total_posts": Post.query.filter_by(company_id=company_id).count(),
        "total_comments": Comment.query.filter_by(company_id=company_id, is_deleted=False).count(),
        "total_contacts": Contact.query.filter_by(company_id=company_id).count(),
        "total_likes": db.session.query(db.func.coalesce(db.func.sum(Post.likes_count), 0)).filter_by(company_id=company_id).scalar(),
        "total_shares": db.session.query(db.func.coalesce(db.func.sum(Post.shares_count), 0)).filter_by(company_id=company_id).scalar(),
        "total_views": db.session.query(db.func.coalesce(db.func.sum(Post.views), 0)).filter_by(company_id=company_id).scalar(),
        "positive_comments": Comment.query.filter_by(company_id=company_id, sentiment="positive").count(),
        "negative_comments": Comment.query.filter_by(company_id=company_id, sentiment="negative").count(),
        "neutral_comments": Comment.query.filter_by(company_id=company_id, sentiment="neutral").count(),
        "lead_comments": Comment.query.filter_by(company_id=company_id, sentiment="lead").count(),
        "unreplied_comments": Comment.query.filter_by(company_id=company_id, is_replied=False, is_deleted=False).count(),
    }

    # Recent comments
    recent_comments = (
        Comment.query.filter_by(company_id=company_id, is_deleted=False)
        .order_by(Comment.commented_at.desc())
        .limit(5)
        .all()
    )

    # Recent contacts
    recent_contacts = (
        Contact.query.filter_by(company_id=company_id)
        .order_by(Contact.created_at.desc())
        .limit(5)
        .all()
    )

    # Connected platforms (only platforms that have at least one connected page)
    connected_platforms = (
        db.session.query(ConnectedPage.platform)
        .filter_by(company_id=company_id, status="connected")
        .distinct()
        .all()
    )
    connected_platforms = [p[0] for p in connected_platforms]

    return render_template(
        "dashboard/index.html",
        stats=stats,
        recent_comments=recent_comments,
        recent_contacts=recent_contacts,
        platforms=PLATFORMS,
        platform_labels=PLATFORM_LABELS,
        connected_platforms=connected_platforms,
    )

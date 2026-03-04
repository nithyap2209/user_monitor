"""Seed script: creates designations (JSON permissions), company, users, and mock data."""

import os
import random
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

from app import create_app
from app.extensions import db
from app.models.user import User
from app.models.company import Company
from app.models.designation import Designation
from app.models.connected_page import ConnectedPage
from app.models.post import Post
from app.models.comment import Comment
from app.models.contact import Contact

# ─── Permission presets ──────────────────────────────────────

SUPER_ADMIN_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": True, "delete": True},
    "comments": {"view": True, "reply": True, "delete": True, "translate": True},
    "contacts": {"view": True, "export": True},
    "analytics": {"view": True},
    "users": {"view": True, "create": True, "delete": True},
    "pages": {"view": True, "connect": True, "disconnect": True},
    "settings": {"view": True, "edit": True},
}

ADMIN_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": True, "delete": True},
    "comments": {"view": True, "reply": True, "delete": True, "translate": True},
    "contacts": {"view": True, "export": True},
    "analytics": {"view": True},
    "users": {"view": True, "create": True, "delete": False},
    "pages": {"view": True, "connect": True, "disconnect": True},
    "settings": {"view": True, "edit": False},
}

SM_MANAGER_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": True, "delete": False},
    "comments": {"view": True, "reply": True, "delete": False, "translate": True},
    "contacts": {"view": True, "export": True},
    "analytics": {"view": True},
    "users": {"view": False, "create": False, "delete": False},
    "pages": {"view": True, "connect": True, "disconnect": False},
    "settings": {"view": False, "edit": False},
}

MARKETING_MANAGER_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": False, "delete": False},
    "comments": {"view": True, "reply": True, "delete": False, "translate": True},
    "contacts": {"view": True, "export": True},
    "analytics": {"view": True},
    "users": {"view": False, "create": False, "delete": False},
    "pages": {"view": True, "connect": False, "disconnect": False},
    "settings": {"view": False, "edit": False},
}

CUSTOMER_SUPPORT_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": False, "delete": False},
    "comments": {"view": True, "reply": True, "delete": False, "translate": True},
    "contacts": {"view": True, "export": False},
    "analytics": {"view": False},
    "users": {"view": False, "create": False, "delete": False},
    "pages": {"view": True, "connect": False, "disconnect": False},
    "settings": {"view": False, "edit": False},
}

SALES_EXEC_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": False, "delete": False},
    "comments": {"view": True, "reply": False, "delete": False, "translate": False},
    "contacts": {"view": True, "export": True},
    "analytics": {"view": True},
    "users": {"view": False, "create": False, "delete": False},
    "pages": {"view": False, "connect": False, "disconnect": False},
    "settings": {"view": False, "edit": False},
}

ANALYST_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": False, "delete": False},
    "comments": {"view": True, "reply": False, "delete": False, "translate": False},
    "contacts": {"view": True, "export": True},
    "analytics": {"view": True},
    "users": {"view": False, "create": False, "delete": False},
    "pages": {"view": True, "connect": False, "disconnect": False},
    "settings": {"view": False, "edit": False},
}

VIEWER_PERMS = {
    "dashboard": {"view": True},
    "posts": {"view": True, "create": False, "delete": False},
    "comments": {"view": True, "reply": False, "delete": False, "translate": False},
    "contacts": {"view": False, "export": False},
    "analytics": {"view": False},
    "users": {"view": False, "create": False, "delete": False},
    "pages": {"view": False, "connect": False, "disconnect": False},
    "settings": {"view": False, "edit": False},
}

# ─── Mock data pools ─────────────────────────────────────────

PLATFORMS = ["facebook", "instagram", "youtube", "linkedin", "twitter", "google_reviews"]

POST_CAPTIONS = [
    "Excited to announce our new product launch! Stay tuned for more details.",
    "Behind the scenes at our office today. What an amazing team!",
    "Our latest blog post covers 10 tips for better social media engagement.",
    "Happy Friday everyone! What are your weekend plans?",
    "We're hiring! Check out our open positions on our website.",
    "Thank you for 10,000 followers! We appreciate every one of you.",
    "New feature alert: We just rolled out dark mode for our app!",
    "Customer spotlight: See how @TechCorp uses our platform.",
    "Join us at the Digital Marketing Summit next week!",
    "Quick tip: Consistent posting leads to better engagement rates.",
    "Throwback to our amazing team retreat last month!",
    "Big news: We've been named a top startup to watch in 2024!",
    "Product update: Faster load times and improved analytics dashboard.",
    "What's your biggest marketing challenge? Let us know in the comments!",
    "Our CEO just published a new article on leadership in tech.",
    "Flash sale! 30% off all plans this weekend only.",
    "We're thrilled to partner with @InnovateHub for an exciting new project.",
    "Milestone: Over 1 million users have signed up for our platform!",
    "Live Q&A session tomorrow at 3 PM EST. Drop your questions below!",
    "New tutorial: How to set up your first campaign in under 5 minutes.",
    "Celebrating our 5th anniversary! Thank you for being part of our journey.",
    "Check out our latest case study on improving ROI with social listening.",
    "Weekend vibes at SocialPulse HQ. Our team works hard and plays hard!",
    "Important: Scheduled maintenance tonight from 11 PM to 2 AM EST.",
    "Just crossed 50,000 comments analyzed this month! AI-powered insights at scale.",
    "Webinar recap: Top takeaways from our social media strategy session.",
    "Feature request? We're all ears! Share your ideas in the comments.",
    "Happy holidays from the SocialPulse team! Wishing you all the best.",
    "Did you know? Our sentiment analysis is 95% accurate across 20+ languages.",
    "New integration: Connect your Slack workspace to get real-time comment alerts.",
]

POSITIVE_COMMENTS = [
    "This is amazing! Love your products!",
    "Great service, will definitely recommend to friends.",
    "Best customer support I've ever experienced!",
    "Absolutely love the new features. Keep it up!",
    "Your team is incredibly talented. Impressive work!",
    "Just signed up and I'm already hooked. Great platform!",
    "Thank you for the quick response. Very professional!",
    "The new update is fantastic. Everything feels so smooth now.",
    "I've been a loyal customer for 3 years and it keeps getting better.",
    "This made my day! Thank you for sharing.",
    "Excellent quality as always. You never disappoint!",
    "Your product literally changed how we do business. Thank you!",
    "So intuitive and easy to use. My whole team loves it.",
    "The best investment we've made this year. Highly recommend!",
    "Wow, the speed improvement is incredible. Night and day difference!",
]

NEGATIVE_COMMENTS = [
    "Terrible experience. Your app keeps crashing!",
    "Very disappointed with the service. Won't be coming back.",
    "The worst customer support ever. No one responds to my tickets.",
    "Your product is overpriced for what it offers.",
    "This is a scam. I want a refund immediately.",
    "I've been waiting for 2 weeks and still no response!",
    "The quality has really gone downhill lately.",
    "Not worth the money at all. Canceling my subscription.",
    "Bugs everywhere! This latest update broke everything.",
    "I regret purchasing this. Total waste of money.",
    "Your app is so slow it's unusable. Fix this ASAP!",
    "Horrible UX. Can't find anything in this new layout.",
    "Lost all my data after the update. This is unacceptable!",
    "False advertising. The product doesn't do half of what you claim.",
    "Never again. Moving to your competitor.",
]

NEUTRAL_COMMENTS = [
    "When is the next update coming out?",
    "Can you share more details about this?",
    "How does this compare to other similar products?",
    "Is there a tutorial for this feature?",
    "What are your business hours?",
    "Do you ship internationally?",
    "I'm considering signing up. What plan do you recommend?",
    "Interesting approach. I'd like to learn more.",
    "Does this work with third-party integrations?",
    "Can someone from the team reach out to me?",
    "What's the pricing for enterprise plans?",
    "I saw this mentioned in a blog post. Checking it out now.",
    "How long has your company been around?",
    "Do you have a referral program?",
    "Noted. I'll keep this in mind.",
]

LEAD_COMMENTS = [
    "Interested! Please send me pricing details to john@example.com",
    "Can someone call me at 555-123-4567? I'd like to discuss a partnership.",
    "We'd love to use this for our company. Contact me at sarah@techfirm.com",
    "This is exactly what we need. My email is mike@startup.io for more info.",
    "Looking to buy in bulk. Please reach out: purchases@bigcorp.com",
    "I want to schedule a demo. You can reach me at 555-987-6543.",
    "Our team needs 50 licenses. Contact our procurement at buy@enterprise.net",
    "Send me a quote! david.smith@consulting.com",
    "We're a marketing agency with 200+ clients. Let's talk. agency@marketpro.com",
    "Need this for our retail chain. 555-444-3333 or retail@shopsmart.com",
]

BUSINESS_COMMENTS = [
    "We sell similar products. Let's collaborate!",
    "Check out our website for complementary services.",
    "As a fellow SaaS company, we'd love to integrate with you.",
    "Our agency can help you with social media management.",
    "We have a partnership opportunity. DM us!",
]

AUTHOR_NAMES = [
    "John Smith", "Sarah Johnson", "Mike Chen", "Emily Davis", "Alex Wilson",
    "Jessica Brown", "David Lee", "Amanda Martinez", "Chris Taylor", "Rachel Anderson",
    "Tom Harris", "Lisa Thompson", "Ryan Clark", "Nicole White", "James Walker",
    "Maria Garcia", "Kevin Robinson", "Laura Adams", "Daniel Young", "Sophia King",
    "Andrew Wright", "Olivia Scott", "Brandon Hill", "Megan Green", "Nathan Baker",
]


def seed():
    app = create_app()
    with app.app_context():
        db.create_all()
        print("Seeding database...\n")

        # ── 1. Designations ──────────────────────────
        designations_data = [
            ("Super Admin", "super_admin", SUPER_ADMIN_PERMS, True),
            ("Admin", "admin", ADMIN_PERMS, True),
            ("Social Media Manager", "social_media_manager", SM_MANAGER_PERMS, True),
            ("Marketing Manager", "marketing_manager", MARKETING_MANAGER_PERMS, True),
            ("Customer Support", "customer_support", CUSTOMER_SUPPORT_PERMS, True),
            ("Sales Executive", "sales_executive", SALES_EXEC_PERMS, True),
            ("Analyst", "analyst", ANALYST_PERMS, True),
            ("Viewer", "viewer", VIEWER_PERMS, True),
        ]

        for name, slug, perms, is_system in designations_data:
            if not Designation.query.filter_by(slug=slug).first():
                d = Designation(name=name, slug=slug, permissions=perms, is_system=is_system)
                db.session.add(d)
                print(f"  + Designation: {name}")

        db.session.commit()

        # ── 2. Company ───────────────────────────────
        company = Company.query.filter_by(slug="demo-company").first()
        if not company:
            company = Company(name="Demo Company", slug="demo-company")
            db.session.add(company)
            db.session.commit()
            print(f"  + Company: Demo Company (id={company.id})")

        # ── 3. Users ─────────────────────────────────
        admin_email = os.getenv("ADMIN_EMAIL", "admin@demo.com")
        admin_password = os.getenv("ADMIN_PASSWORD", "admin123")

        super_admin_d = Designation.query.filter_by(slug="super_admin").first()

        if not User.query.filter_by(email=admin_email).first():
            admin = User(
                email=admin_email,
                full_name="Admin User",
                company_id=company.id,
                designation_id=super_admin_d.id,
            )
            admin.set_password(admin_password)
            db.session.add(admin)
            print(f"  + Super Admin: {admin_email} / {admin_password}")

        # Create sample users with different roles
        sample_users = [
            ("manager@demo.com", "Manager User", "social_media_manager"),
            ("support@demo.com", "Support Agent", "customer_support"),
            ("sales@demo.com", "Sales Rep", "sales_executive"),
            ("viewer@demo.com", "Viewer User", "viewer"),
        ]
        for email, name, role_slug in sample_users:
            if not User.query.filter_by(email=email).first():
                d = Designation.query.filter_by(slug=role_slug).first()
                u = User(email=email, full_name=name, company_id=company.id, designation_id=d.id)
                u.set_password("demo123")
                db.session.add(u)
                print(f"  + User: {email} / demo123 ({role_slug})")

        db.session.commit()

        # ── 4. Connected Pages ────────────────────────
        pages_data = [
            ("facebook", "DemoCompany", "123456789"),
            ("instagram", "demo_company", "987654321"),
            ("youtube", "Demo Company Channel", "UC_DEMO_123"),
            ("linkedin", "Demo Company", "demo-company-li"),
            ("twitter", "@democompany", "12345678"),
            ("google_reviews", "Demo Company - Main Office", "ChIJ_DEMO_123"),
        ]

        connected_pages = []
        for platform, name, pid in pages_data:
            page = ConnectedPage.query.filter_by(company_id=company.id, platform=platform).first()
            if not page:
                page = ConnectedPage(
                    company_id=company.id,
                    platform=platform,
                    page_name=name,
                    page_id=pid,
                    status="connected",
                    followers_count=random.randint(500, 50000),
                    last_synced_at=datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 48)),
                )
                db.session.add(page)
                print(f"  + Page: {platform} - {name}")
            connected_pages.append(page)

        db.session.commit()
        # Refresh to get IDs
        connected_pages = ConnectedPage.query.filter_by(company_id=company.id).all()

        # ── 5. Posts (60+) ────────────────────────────
        if Post.query.filter_by(company_id=company.id).count() == 0:
            posts = []
            for i in range(60):
                page = random.choice(connected_pages)
                posted_at = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 60), hours=random.randint(0, 23))
                post = Post(
                    company_id=company.id,
                    connected_page_id=page.id,
                    platform=page.platform,
                    platform_post_id=f"post_{page.platform}_{i}",
                    caption=random.choice(POST_CAPTIONS),
                    media_type=random.choice(["image", "video", "text", "carousel"]),
                    likes_count=random.randint(5, 2000),
                    comments_count=0,  # Will be updated after comments are created
                    shares_count=random.randint(0, 500),
                    reach=random.randint(100, 50000),
                    views=random.randint(50, 100000),
                    posted_at=posted_at,
                )
                db.session.add(post)
                posts.append(post)

            db.session.commit()
            print(f"  + {len(posts)} posts created")

            # ── 6. Comments (500+) ────────────────────────
            posts = Post.query.filter_by(company_id=company.id).all()
            comment_count = 0

            for post in posts:
                num_comments = random.randint(3, 15)
                for j in range(num_comments):
                    # Choose sentiment category
                    roll = random.random()
                    if roll < 0.30:
                        text = random.choice(POSITIVE_COMMENTS)
                        sentiment = "positive"
                        score = random.uniform(0.7, 1.0)
                    elif roll < 0.50:
                        text = random.choice(NEGATIVE_COMMENTS)
                        sentiment = "negative"
                        score = random.uniform(0.0, 0.3)
                    elif roll < 0.75:
                        text = random.choice(NEUTRAL_COMMENTS)
                        sentiment = "neutral"
                        score = random.uniform(0.4, 0.6)
                    elif roll < 0.90:
                        text = random.choice(LEAD_COMMENTS)
                        sentiment = "lead"
                        score = random.uniform(0.5, 0.8)
                        has_contact = True
                    else:
                        text = random.choice(BUSINESS_COMMENTS)
                        sentiment = "business"
                        score = random.uniform(0.4, 0.7)

                    has_contact = sentiment == "lead"
                    commented_at = post.posted_at + timedelta(hours=random.randint(1, 72)) if post.posted_at else datetime.now(timezone.utc)
                    author = random.choice(AUTHOR_NAMES)

                    comment = Comment(
                        post_id=post.id,
                        company_id=company.id,
                        platform=post.platform,
                        platform_comment_id=f"comment_{post.platform}_{post.id}_{j}",
                        author_name=author,
                        comment_text=text,
                        sentiment=sentiment,
                        sentiment_score=round(score, 2),
                        detected_language="en",
                        has_contact_info=has_contact,
                        is_replied=random.random() < 0.3,
                        is_flagged=sentiment == "negative" and random.random() < 0.3,
                        commented_at=commented_at,
                    )
                    db.session.add(comment)
                    comment_count += 1

                # Update post comment count
                post.comments_count = num_comments

            db.session.commit()
            print(f"  + {comment_count} comments created")

            # ── 7. Contacts from lead comments ─────────────
            lead_comments = Comment.query.filter_by(company_id=company.id, sentiment="lead").all()
            contact_count = 0

            for lc in lead_comments:
                # Extract mock contact info
                import re
                emails = re.findall(r"[\w.+-]+@[\w.-]+\.\w+", lc.comment_text)
                phones = re.findall(r"\d{3}[-.]?\d{3}[-.]?\d{4}", lc.comment_text)

                contact = Contact(
                    company_id=company.id,
                    comment_id=lc.id,
                    source_post_id=lc.post_id,
                    name=lc.author_name,
                    email=emails[0] if emails else None,
                    phone=phones[0] if phones else None,
                    platform=lc.platform,
                    contact_type="lead",
                )
                db.session.add(contact)
                contact_count += 1

            # Add some manual contacts
            for i in range(10):
                contact = Contact(
                    company_id=company.id,
                    name=random.choice(AUTHOR_NAMES),
                    email=f"contact{i}@example.com",
                    phone=f"555-{random.randint(100,999)}-{random.randint(1000,9999)}",
                    contact_type=random.choice(["lead", "business", "manual"]),
                    platform=random.choice(PLATFORMS),
                )
                db.session.add(contact)
                contact_count += 1

            db.session.commit()
            print(f"  + {contact_count} contacts created")

        print("\nSeed complete!")
        print(f"\n  Login: {admin_email} / {admin_password}")
        print("  Other users: manager@demo.com, support@demo.com, sales@demo.com, viewer@demo.com (password: demo123)")


if __name__ == "__main__":
    seed()

"""Instagram Graph API integration.

Supports both:
- Facebook Page tokens (legacy IG Business accounts linked to FB Pages)
- Instagram Login tokens (IGAAS...) via graph.instagram.com
"""

import requests
from app.models.company_api_key import CompanyAPIKey

FB_GRAPH_API_BASE = "https://graph.facebook.com/v21.0"
IG_GRAPH_API_BASE = "https://graph.instagram.com/v21.0"


def _api_base_for_token(token):
    """Pick the right Graph API host based on token type."""
    if token and token.startswith("IGAAS"):
        return IG_GRAPH_API_BASE
    return FB_GRAPH_API_BASE


class InstagramService:
    def __init__(self, company_id):
        self.company_id = company_id
        self.credentials = CompanyAPIKey.get_for_company(company_id, "instagram")

    @property
    def is_configured(self):
        return self.credentials is not None and self.credentials.access_token is not None

    def test_connection(self):
        """Verify the access token by fetching the IG business account info."""
        if not self.is_configured:
            return {"success": False, "error": "Access token not configured."}
        try:
            token = self.credentials.access_token
            base = _api_base_for_token(token)
            resp = requests.get(
                f"{base}/me",
                params={"access_token": token, "fields": "id,name"},
                timeout=10,
            )
            data = resp.json()
            if "error" in data:
                return {"success": False, "error": data["error"].get("message", "Unknown error")}
            return {"success": True, "name": data.get("name"), "id": data.get("id")}
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def fetch_media(self, ig_user_id, limit=25, token=None):
        """Fetch recent media for an Instagram business account."""
        token = token or (self.credentials.access_token if self.is_configured else None)
        if not token:
            return []
        try:
            base = _api_base_for_token(token)
            resp = requests.get(
                f"{base}/{ig_user_id}/media",
                params={
                    "access_token": token,
                    "fields": "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink,like_count,comments_count",
                    "limit": limit,
                },
                timeout=15,
            )
            data = resp.json()
            return data.get("data", [])
        except requests.RequestException:
            return []

    def fetch_comments(self, media_id, limit=100, token=None):
        """Fetch comments for a media post."""
        token = token or (self.credentials.access_token if self.is_configured else None)
        if not token:
            return []
        try:
            base = _api_base_for_token(token)
            resp = requests.get(
                f"{base}/{media_id}/comments",
                params={
                    "access_token": token,
                    "fields": "id,text,from,timestamp,like_count",
                    "limit": limit,
                },
                timeout=15,
            )
            data = resp.json()
            return data.get("data", [])
        except requests.RequestException:
            return []

    def reply_to_comment(self, comment_id, message, token=None):
        token = token or (self.credentials.access_token if self.is_configured else None)
        if not token:
            return False
        try:
            base = _api_base_for_token(token)
            resp = requests.post(
                f"{base}/{comment_id}/replies",
                params={"access_token": token},
                json={"message": message},
                timeout=10,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

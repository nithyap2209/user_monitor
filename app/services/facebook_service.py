"""Facebook Graph API integration.

All methods accept company_id to scope data and look up API credentials
from CompanyAPIKey.
"""

import requests
from app.models.company_api_key import CompanyAPIKey

GRAPH_API_BASE = "https://graph.facebook.com/v21.0"


class FacebookService:
    def __init__(self, company_id):
        self.company_id = company_id
        self.credentials = CompanyAPIKey.get_for_company(company_id, "facebook")

    @property
    def is_configured(self):
        return self.credentials is not None and self.credentials.access_token is not None

    def test_connection(self):
        """Verify the access token by calling /me endpoint."""
        if not self.is_configured:
            return {"success": False, "error": "Access token not configured."}
        try:
            resp = requests.get(
                f"{GRAPH_API_BASE}/me",
                params={"access_token": self.credentials.access_token, "fields": "id,name"},
                timeout=10,
            )
            data = resp.json()
            if "error" in data:
                return {"success": False, "error": data["error"].get("message", "Unknown error")}
            return {"success": True, "name": data.get("name"), "id": data.get("id")}
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def fetch_page_info(self, page_id):
        """Fetch page metadata from Facebook Graph API."""
        if not self.is_configured:
            return None
        try:
            resp = requests.get(
                f"{GRAPH_API_BASE}/{page_id}",
                params={
                    "access_token": self.credentials.access_token,
                    "fields": "id,name,followers_count,fan_count,picture",
                },
                timeout=10,
            )
            data = resp.json()
            if "error" in data:
                return None
            return data
        except requests.RequestException:
            return None

    def fetch_posts(self, page_id, limit=25):
        """Fetch recent posts for a page."""
        if not self.is_configured:
            return []
        try:
            resp = requests.get(
                f"{GRAPH_API_BASE}/{page_id}/posts",
                params={
                    "access_token": self.credentials.access_token,
                    "fields": "id,message,created_time,full_picture,shares,permalink_url",
                    "limit": limit,
                },
                timeout=60,
            )
            data = resp.json()
            return data.get("data", [])
        except requests.RequestException:
            return []

    def fetch_comments(self, post_id, limit=100):
        """Fetch comments for a post."""
        if not self.is_configured:
            return []
        try:
            resp = requests.get(
                f"{GRAPH_API_BASE}/{post_id}/comments",
                params={
                    "access_token": self.credentials.access_token,
                    "fields": "id,from,message,created_time,like_count",
                    "limit": limit,
                },
                timeout=60,
            )
            data = resp.json()
            return data.get("data", [])
        except requests.RequestException:
            return []

    def reply_to_comment(self, comment_id, message):
        """Post a reply to a comment."""
        if not self.is_configured:
            return False
        try:
            resp = requests.post(
                f"{GRAPH_API_BASE}/{comment_id}/comments",
                params={"access_token": self.credentials.access_token},
                json={"message": message},
                timeout=10,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def delete_comment(self, comment_id):
        """Delete a comment."""
        if not self.is_configured:
            return False
        try:
            resp = requests.delete(
                f"{GRAPH_API_BASE}/{comment_id}",
                params={"access_token": self.credentials.access_token},
                timeout=10,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def hide_comment(self, comment_id, hide=True):
        """Hide or unhide a comment."""
        if not self.is_configured:
            return False
        try:
            resp = requests.post(
                f"{GRAPH_API_BASE}/{comment_id}",
                params={"access_token": self.credentials.access_token},
                json={"is_hidden": hide},
                timeout=10,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

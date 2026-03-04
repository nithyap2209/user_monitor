"""LinkedIn API integration."""

import requests
from app.models.company_api_key import CompanyAPIKey

LI_API_BASE = "https://api.linkedin.com/v2"


class LinkedInService:
    def __init__(self, company_id):
        self.company_id = company_id
        self.credentials = CompanyAPIKey.get_for_company(company_id, "linkedin")

    @property
    def is_configured(self):
        return self.credentials is not None and self.credentials.access_token is not None

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.credentials.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
        }

    def test_connection(self):
        """Verify the access token by fetching the current user profile."""
        if not self.is_configured:
            return {"success": False, "error": "Access token not configured."}
        try:
            resp = requests.get(
                f"{LI_API_BASE}/me",
                headers=self._headers(),
                timeout=10,
            )
            if resp.status_code == 401:
                return {"success": False, "error": "Invalid or expired access token."}
            data = resp.json()
            if "id" in data:
                name = f"{data.get('localizedFirstName', '')} {data.get('localizedLastName', '')}".strip()
                return {"success": True, "name": name or data["id"], "id": data["id"]}
            return {"success": False, "error": data.get("message", "Unknown error")}
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def fetch_posts(self, org_id, limit=25):
        if not self.is_configured:
            return []
        try:
            resp = requests.get(
                f"{LI_API_BASE}/ugcPosts",
                headers=self._headers(),
                params={"q": "authors", "authors": f"List(urn:li:organization:{org_id})", "count": limit},
                timeout=15,
            )
            data = resp.json()
            return data.get("elements", [])
        except requests.RequestException:
            return []

    def fetch_comments(self, post_urn, limit=100):
        if not self.is_configured:
            return []
        try:
            resp = requests.get(
                f"{LI_API_BASE}/socialActions/{post_urn}/comments",
                headers=self._headers(),
                params={"count": limit},
                timeout=15,
            )
            data = resp.json()
            return data.get("elements", [])
        except requests.RequestException:
            return []

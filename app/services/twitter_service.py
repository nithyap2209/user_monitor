"""X/Twitter API v2 integration."""

import requests
from app.models.company_api_key import CompanyAPIKey

TWITTER_API_BASE = "https://api.twitter.com/2"


class TwitterService:
    def __init__(self, company_id):
        self.company_id = company_id
        self.credentials = CompanyAPIKey.get_for_company(company_id, "twitter")

    @property
    def is_configured(self):
        return self.credentials is not None and (
            self.credentials.access_token is not None or self.credentials.api_key is not None
        )

    def _headers(self):
        if self.credentials.access_token:
            return {"Authorization": f"Bearer {self.credentials.access_token}"}
        return {}

    def test_connection(self):
        """Verify credentials by fetching the authenticated user."""
        if not self.is_configured:
            return {"success": False, "error": "Bearer token not configured."}
        try:
            resp = requests.get(
                f"{TWITTER_API_BASE}/users/me",
                headers=self._headers(),
                timeout=10,
            )
            data = resp.json()
            if "errors" in data:
                msg = data["errors"][0].get("message", "Unknown error")
                return {"success": False, "error": msg}
            if "data" in data:
                return {
                    "success": True,
                    "name": data["data"].get("name"),
                    "id": data["data"].get("id"),
                    "username": data["data"].get("username"),
                }
            return {"success": False, "error": "Unexpected response."}
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def fetch_tweets(self, user_id, limit=25):
        if not self.is_configured:
            return []
        try:
            resp = requests.get(
                f"{TWITTER_API_BASE}/users/{user_id}/tweets",
                headers=self._headers(),
                params={
                    "max_results": min(limit, 100),
                    "tweet.fields": "created_at,public_metrics,text",
                },
                timeout=15,
            )
            data = resp.json()
            return data.get("data", [])
        except requests.RequestException:
            return []

    def fetch_replies(self, tweet_id, limit=100):
        if not self.is_configured:
            return []
        try:
            resp = requests.get(
                f"{TWITTER_API_BASE}/tweets/search/recent",
                headers=self._headers(),
                params={
                    "query": f"conversation_id:{tweet_id}",
                    "max_results": min(limit, 100),
                    "tweet.fields": "created_at,author_id,text,public_metrics",
                },
                timeout=15,
            )
            data = resp.json()
            return data.get("data", [])
        except requests.RequestException:
            return []

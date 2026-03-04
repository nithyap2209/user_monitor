"""Google Business Profile / Reviews API integration."""

import requests
from app.models.company_api_key import CompanyAPIKey

GBP_API_BASE = "https://mybusiness.googleapis.com/v4"


class GoogleReviewsService:
    def __init__(self, company_id):
        self.company_id = company_id
        self.credentials = CompanyAPIKey.get_for_company(company_id, "google_reviews")

    @property
    def is_configured(self):
        return self.credentials is not None and self.credentials.access_token is not None

    def _headers(self):
        return {"Authorization": f"Bearer {self.credentials.access_token}"}

    def test_connection(self):
        """Verify credentials by listing accounts."""
        if not self.is_configured:
            return {"success": False, "error": "Access token not configured."}
        try:
            resp = requests.get(
                f"{GBP_API_BASE}/accounts",
                headers=self._headers(),
                timeout=10,
            )
            if resp.status_code == 401:
                return {"success": False, "error": "Invalid or expired access token."}
            data = resp.json()
            if "error" in data:
                return {"success": False, "error": data["error"].get("message", "Unknown error")}
            accounts = data.get("accounts", [])
            if accounts:
                return {"success": True, "name": accounts[0].get("accountName", ""), "id": accounts[0].get("name", "")}
            return {"success": True, "name": "Connected (no accounts found)", "id": ""}
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def fetch_reviews(self, location_id, limit=50):
        if not self.is_configured:
            return []
        try:
            resp = requests.get(
                f"{GBP_API_BASE}/{location_id}/reviews",
                headers=self._headers(),
                params={"pageSize": limit},
                timeout=15,
            )
            data = resp.json()
            return data.get("reviews", [])
        except requests.RequestException:
            return []

    def reply_to_review(self, review_id, message):
        if not self.is_configured:
            return False
        try:
            resp = requests.put(
                f"{GBP_API_BASE}/{review_id}/reply",
                headers=self._headers(),
                json={"comment": message},
                timeout=10,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

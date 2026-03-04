"""YouTube Data API v3 integration."""

import requests
from app.models.company_api_key import CompanyAPIKey

YT_API_BASE = "https://www.googleapis.com/youtube/v3"


class YouTubeService:
    def __init__(self, company_id):
        self.company_id = company_id
        self.credentials = CompanyAPIKey.get_for_company(company_id, "youtube")

    @property
    def is_configured(self):
        return self.credentials is not None and self.credentials.api_key is not None

    def test_connection(self):
        """Verify the API key by fetching a channel snippet."""
        if not self.is_configured:
            return {"success": False, "error": "API key not configured."}
        try:
            channel_id = self.credentials.page_id
            params = {"key": self.credentials.api_key, "part": "snippet"}
            if channel_id:
                params["id"] = channel_id
            else:
                params["mine"] = "true"
                if self.credentials.access_token:
                    params.pop("key", None)

            headers = {}
            if self.credentials.access_token:
                headers["Authorization"] = f"Bearer {self.credentials.access_token}"

            resp = requests.get(f"{YT_API_BASE}/channels", params=params, headers=headers, timeout=10)
            data = resp.json()
            if "error" in data:
                msg = data["error"].get("message", "Unknown error")
                return {"success": False, "error": msg}
            items = data.get("items", [])
            if items:
                snippet = items[0].get("snippet", {})
                return {"success": True, "name": snippet.get("title"), "id": items[0].get("id")}
            return {"success": False, "error": "No channel found. Check channel ID."}
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

    def fetch_all_channel_videos(self, channel_id):
        """Fetch ALL videos from a YouTube channel using the playlistItems API.

        Uses the channel's "uploads" playlist (UU...) which is far more
        efficient than the search API (1 quota unit vs 100 per page) and
        has no result cap — it paginates through every video on the channel.
        """
        if not self.is_configured:
            return []

        # Convert channel ID to uploads playlist ID (UC... -> UU...)
        if channel_id.startswith("UC"):
            uploads_playlist_id = "UU" + channel_id[2:]
        else:
            uploads_playlist_id = channel_id

        all_items = []
        page_token = None

        while True:
            try:
                params = {
                    "key": self.credentials.api_key,
                    "playlistId": uploads_playlist_id,
                    "part": "snippet",
                    "maxResults": 50,
                }
                if page_token:
                    params["pageToken"] = page_token

                resp = requests.get(
                    f"{YT_API_BASE}/playlistItems",
                    params=params,
                    timeout=15,
                )
                data = resp.json()

                if "error" in data:
                    break

                items = data.get("items", [])
                if not items:
                    break

                all_items.extend(items)
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
            except requests.RequestException:
                break

        return all_items

    def fetch_channel_videos(self, channel_id, limit=500, keyword=None):
        """Search videos from a YouTube channel by keyword.

        Uses the YouTube search API which returns max 50 per page.
        This method follows nextPageToken to fetch up to `limit` videos.
        """
        if not self.is_configured:
            return []

        all_items = []
        page_token = None

        while len(all_items) < limit:
            try:
                params = {
                    "key": self.credentials.api_key,
                    "channelId": channel_id,
                    "part": "snippet",
                    "order": "date" if not keyword else "relevance",
                    "type": "video",
                    "maxResults": min(50, limit - len(all_items)),
                }
                if keyword:
                    params["q"] = keyword
                if page_token:
                    params["pageToken"] = page_token

                resp = requests.get(
                    f"{YT_API_BASE}/search",
                    params=params,
                    timeout=15,
                )
                data = resp.json()

                if "error" in data:
                    break

                items = data.get("items", [])
                if not items:
                    break

                all_items.extend(items)
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
            except requests.RequestException:
                break

        return all_items

    def fetch_comments(self, video_id, include_replies=True):
        """Fetch ALL comment threads for a video, following every page until done.

        When include_replies=True, requests part=snippet,replies so each thread
        also carries inline reply comments at no extra quota cost.
        """
        if not self.is_configured:
            return []

        all_items = []
        page_token = None
        part = "snippet,replies" if include_replies else "snippet"

        while True:
            try:
                params = {
                    "key": self.credentials.api_key,
                    "videoId": video_id,
                    "part": part,
                    "maxResults": 100,
                    "order": "time",
                }
                if page_token:
                    params["pageToken"] = page_token

                resp = requests.get(
                    f"{YT_API_BASE}/commentThreads",
                    params=params,
                    timeout=30,
                )
                data = resp.json()

                if "error" in data:
                    break

                items = data.get("items", [])
                if not items:
                    break

                all_items.extend(items)
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
            except requests.RequestException:
                break

        return all_items

    def fetch_comment_replies(self, parent_id):
        """Fetch ALL replies for a top-level comment thread, following every page.

        Used when a thread's totalReplyCount exceeds the inline replies
        returned by the commentThreads API (max ~20 inline).
        """
        if not self.is_configured:
            return []

        all_items = []
        page_token = None

        while True:
            try:
                params = {
                    "key": self.credentials.api_key,
                    "parentId": parent_id,
                    "part": "snippet",
                    "maxResults": 100,
                }
                if page_token:
                    params["pageToken"] = page_token

                resp = requests.get(
                    f"{YT_API_BASE}/comments",
                    params=params,
                    timeout=30,
                )
                data = resp.json()

                if "error" in data:
                    break

                items = data.get("items", [])
                if not items:
                    break

                all_items.extend(items)
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
            except requests.RequestException:
                break

        return all_items

    def reply_to_comment(self, parent_id, message):
        if not self.is_configured or not self.credentials.access_token:
            return False
        try:
            resp = requests.post(
                f"{YT_API_BASE}/comments",
                params={"part": "snippet"},
                headers={"Authorization": f"Bearer {self.credentials.access_token}"},
                json={
                    "snippet": {
                        "parentId": parent_id,
                        "textOriginal": message,
                    }
                },
                timeout=10,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

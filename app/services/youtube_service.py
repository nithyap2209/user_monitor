"""YouTube Data API v3 integration."""

import logging
import requests
from datetime import datetime, timezone, timedelta
from app.models.company_api_key import CompanyAPIKey

logger = logging.getLogger(__name__)

YT_API_BASE = "https://www.googleapis.com/youtube/v3"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

# Reusable session for connection pooling
_yt_session = requests.Session()
_yt_session.headers.update({"Connection": "keep-alive"})
_yt_adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=2)
_yt_session.mount("https://", _yt_adapter)


class YouTubeService:
    def __init__(self, company_id):
        self.company_id = company_id
        self.credentials = CompanyAPIKey.get_for_company(company_id, "youtube")

    @property
    def is_configured(self):
        return self.credentials is not None and self.credentials.api_key is not None

    def _ensure_valid_token(self):
        """Refresh the OAuth access token if expired. Returns True if token is valid."""
        creds = self.credentials
        if not creds or not creds.access_token:
            return False

        # Check if token is expired (with 5-minute buffer)
        if creds.token_expires_at:
            expires = creds.token_expires_at
            # Make naive datetimes from DB timezone-aware (assume UTC)
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
            if expires > datetime.now(timezone.utc) + timedelta(minutes=5):
                return True  # Token still valid

        # No refresh token → can't refresh
        if not creds.refresh_token:
            logger.warning("YouTube OAuth token expired and no refresh token available.")
            return bool(creds.access_token)  # Try anyway, might still work

        # Get OAuth client credentials
        extra = creds.extra_data or {}
        oauth_client_id = extra.get("oauth_client_id")
        oauth_client_secret = creds.api_secret

        if not oauth_client_id or not oauth_client_secret:
            logger.warning("Cannot refresh YouTube token: OAuth client credentials missing.")
            return bool(creds.access_token)

        # Refresh the token
        try:
            resp = _yt_session.post(GOOGLE_TOKEN_URL, data={
                "client_id": oauth_client_id,
                "client_secret": oauth_client_secret,
                "refresh_token": creds.refresh_token,
                "grant_type": "refresh_token",
            }, timeout=15)
            data = resp.json()

            if "access_token" in data:
                from app.extensions import db
                creds.access_token = data["access_token"]
                expires_in = data.get("expires_in", 3600)
                creds.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
                db.session.commit()
                logger.info("YouTube OAuth token refreshed successfully.")
                return True
            else:
                logger.error("YouTube token refresh failed: %s", data.get("error_description", data))
                return bool(creds.access_token)
        except Exception as e:
            logger.error("YouTube token refresh error: %s", e)
            return bool(creds.access_token)

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

            resp = _yt_session.get(f"{YT_API_BASE}/channels", params=params, headers=headers, timeout=10)
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

    def fetch_all_channel_videos(self, channel_id, known_video_ids=None):
        """Fetch videos from a YouTube channel using the playlistItems API.

        Uses the channel's "uploads" playlist (UU...) which is far more
        efficient than the search API (1 quota unit vs 100 per page).

        When known_video_ids is provided (re-sync), stops fetching once it
        encounters a video already in the set — since the uploads playlist
        is newest-first, all remaining videos are already known.

        Returns (items_list, error_string_or_None).
        """
        if not self.is_configured:
            return [], "API key not configured."

        # Convert channel ID to uploads playlist ID (UC... -> UU...)
        if channel_id.startswith("UC"):
            uploads_playlist_id = "UU" + channel_id[2:]
        else:
            uploads_playlist_id = channel_id

        all_items = []
        page_token = None
        last_error = None
        stop_early = False

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

                resp = _yt_session.get(
                    f"{YT_API_BASE}/playlistItems",
                    params=params,
                    timeout=15,
                )
                data = resp.json()

                if "error" in data:
                    last_error = data["error"].get("message", "Unknown YouTube API error")
                    break

                items = data.get("items", [])
                if not items:
                    break

                # Smart stop: if we hit known videos, stop fetching
                if known_video_ids:
                    for item in items:
                        vid = item.get("snippet", {}).get("resourceId", {}).get("videoId")
                        if not vid:
                            vid_raw = item.get("id")
                            vid = vid_raw.get("videoId") if isinstance(vid_raw, dict) else vid_raw
                        if vid and vid in known_video_ids:
                            stop_early = True
                            break
                        all_items.append(item)
                    if stop_early:
                        break
                else:
                    all_items.extend(items)

                page_token = data.get("nextPageToken")
                if not page_token:
                    break
            except requests.RequestException as e:
                last_error = str(e)
                break

        return all_items, last_error

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

                resp = _yt_session.get(
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

                resp = _yt_session.get(
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

                resp = _yt_session.get(
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
        """Post a reply to a YouTube comment.

        Returns a dict: {"success": bool, "error": str or None}
        """
        if not self.is_configured:
            return {"success": False, "error": "YouTube API key not configured."}
        if not self.credentials.access_token:
            return {"success": False, "error": "YouTube OAuth token missing. Connect YouTube OAuth first."}

        # Ensure token is valid (refresh if expired)
        self._ensure_valid_token()

        try:
            # Note: Don't send API key with OAuth — use only Bearer token for write ops
            resp = _yt_session.post(
                f"{YT_API_BASE}/comments",
                params={"part": "snippet"},
                headers={
                    "Authorization": f"Bearer {self.credentials.access_token}",
                    "Content-Type": "application/json",
                },
                json={
                    "snippet": {
                        "parentId": parent_id,
                        "textOriginal": message,
                    }
                },
                timeout=15,
            )

            if resp.status_code in (200, 201):
                logger.info("YouTube reply posted successfully to parent=%s", parent_id)
                return {"success": True, "error": None}

            # Parse YouTube error
            try:
                err_data = resp.json()
                err_msg = err_data.get("error", {}).get("message", resp.text[:200])
                err_reason = ""
                errors = err_data.get("error", {}).get("errors", [])
                if errors:
                    err_reason = errors[0].get("reason", "")
            except Exception:
                err_msg = resp.text[:200]
                err_reason = ""

            logger.error(
                "YouTube reply failed (HTTP %s, reason=%s): %s",
                resp.status_code, err_reason, err_msg,
            )

            # User-friendly messages for common errors
            if resp.status_code == 401:
                return {"success": False, "error": "OAuth token expired or invalid. Reconnect YouTube OAuth."}
            elif resp.status_code == 403:
                if "forbidden" in err_reason.lower() or "insufficient" in err_msg.lower():
                    return {"success": False, "error": "Permission denied. Make sure you authorized with the YouTube channel owner account."}
                if "commentsDisabled" in err_reason:
                    return {"success": False, "error": "Comments are disabled on this video."}
                return {"success": False, "error": f"YouTube API forbidden: {err_msg}"}
            elif resp.status_code == 404:
                return {"success": False, "not_found": True, "error": "This comment no longer exists on YouTube (deleted by the author or YouTube)."}
            else:
                return {"success": False, "error": f"YouTube API error ({resp.status_code}): {err_msg}"}

        except requests.RequestException as e:
            logger.error("YouTube reply request error: %s", e)
            return {"success": False, "error": f"Network error: {str(e)[:100]}"}

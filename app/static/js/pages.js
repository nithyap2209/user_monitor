// Connected pages interactions

var currentSyncPageId = null;
var searchResultVideos = [];
var currentSyncPlatform = null;
var currentSyncPageName = null;
var syncProgressInterval = null;

var PLATFORM_LABELS = {facebook: 'Facebook', instagram: 'Instagram', youtube: 'YouTube', linkedin: 'LinkedIn', twitter: 'X / Twitter'};
var PLATFORM_ICONS = {facebook: 'fa-facebook-f', instagram: 'fa-instagram', youtube: 'fa-youtube', linkedin: 'fa-linkedin-in', twitter: 'fa-twitter'};
var PLATFORM_COLORS = {facebook: 'text-blue-600', instagram: 'text-pink-500', youtube: 'text-red-600', linkedin: 'text-blue-700', twitter: 'text-gray-800'};

function formatCount(n) {
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return n.toString();
}

function escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text || '';
    return div.innerHTML;
}

// ── Panel helpers ──────────────────────────────────────────

function showPanel(panelId) {
    ['ytOptionPanel', 'ytSearchPanel', 'ytSelectionPanel', 'syncProgressPanel'].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) {
            if (id === panelId) {
                el.classList.remove('hidden');
                el.style.display = 'flex';
            } else {
                el.classList.add('hidden');
                el.style.display = '';
            }
        }
    });
}

function openYouTubeSearchPanel() {
    document.getElementById('ytKeywordInput').value = '';
    showPanel('ytSearchPanel');
}

function startYTGetAllVideos() {
    showSyncProgressPanel();
    startSyncStream(currentSyncPageId, null);
}

function showSyncProgressPanel() {
    var platformLabel = PLATFORM_LABELS[currentSyncPlatform] || currentSyncPlatform;
    var platformIcon = PLATFORM_ICONS[currentSyncPlatform] || 'fa-globe';
    var platformColor = PLATFORM_COLORS[currentSyncPlatform] || 'text-gray-600';
    var pageName = currentSyncPageName || 'Page';
    var itemWord = currentSyncPlatform === 'youtube' ? 'videos' : 'posts';

    document.getElementById('syncTitle').textContent = 'Syncing ' + pageName;
    document.getElementById('syncSubtitle').innerHTML =
        '<i class="fab ' + platformIcon + ' ' + platformColor + ' mr-1"></i>' +
        platformLabel + ' — Fetching ' + itemWord + ' and comments';
    document.getElementById('syncProgressText').textContent = 'Starting sync...';
    document.getElementById('syncProgressPercent').textContent = '0%';
    document.getElementById('syncProgressBar').style.width = '0%';
    document.getElementById('syncProgressBar').className = 'bg-indigo-600 h-2 rounded-full transition-all duration-300';
    document.getElementById('syncVideoList').innerHTML = '';
    document.getElementById('syncFooter').classList.add('hidden');
    document.getElementById('syncCloseBtn').classList.add('hidden');
    document.getElementById('syncHeaderIcon').innerHTML =
        '<i class="fab ' + platformIcon + ' ' + platformColor + ' animate-spin"></i>';

    if (syncProgressInterval) { clearInterval(syncProgressInterval); syncProgressInterval = null; }

    showPanel('syncProgressPanel');
}

// ── Main entry point ───────────────────────────────────────

async function triggerSync(pageId, platform, pageName) {
    currentSyncPageId = pageId;
    currentSyncPlatform = platform;
    currentSyncPageName = pageName || 'Page';
    searchResultVideos = [];

    var modal = document.getElementById('syncModal');
    if (modal) {
        modal.classList.remove('hidden');
        document.body.style.overflow = 'hidden';
    }

    if (platform === 'youtube') {
        // YouTube: show option panel (Get All Videos / Search Videos)
        showPanel('ytOptionPanel');
    } else {
        // All other platforms: go straight to streaming sync
        showSyncProgressPanel();
        await startSyncStream(pageId, null);
    }
}

// ── YouTube keyword search ─────────────────────────────────

async function searchYouTubeVideos() {
    var keyword = document.getElementById('ytKeywordInput').value.trim();
    if (!keyword) {
        showToast('Please enter a keyword.', 'warning');
        return;
    }

    var searchBtn = document.getElementById('ytSearchBtn');
    setButtonLoading(searchBtn, true);

    try {
        var resp = await fetch('/api/sync/' + currentSyncPageId + '/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken,
            },
            body: JSON.stringify({ keyword: keyword, limit: 50 }),
        });

        var data = await resp.json();
        if (data.error) {
            showToast(data.error, 'danger');
            return;
        }

        if (!data.videos || data.videos.length === 0) {
            showToast('No videos found for "' + keyword + '". Try a different keyword.', 'warning');
            return;
        }

        searchResultVideos = data.videos;
        renderVideoSelectionPanel(data.videos, keyword);
    } catch (e) {
        showToast('Search failed: ' + e.message, 'danger');
    } finally {
        setButtonLoading(searchBtn, false);
    }
}

// ── Video selection panel ──────────────────────────────────

function renderVideoSelectionPanel(videos, keyword) {
    document.getElementById('ytSelectionSubtitle').textContent =
        videos.length + ' video' + (videos.length !== 1 ? 's' : '') + ' found for "' + keyword + '"';

    document.getElementById('ytSelectAll').checked = false;

    var list = document.getElementById('ytVideoList');
    list.innerHTML = '';

    for (var i = 0; i < videos.length; i++) {
        var v = videos[i];
        var publishedDate = '';
        if (v.published_at) {
            try {
                publishedDate = new Date(v.published_at).toLocaleDateString('en-US', {
                    year: 'numeric', month: 'short', day: 'numeric'
                });
            } catch (e) {
                publishedDate = v.published_at;
            }
        }

        var card = document.createElement('label');
        card.className = 'flex items-center gap-3 p-3 rounded-lg border bg-white mb-2 cursor-pointer hover:bg-indigo-50 hover:border-indigo-200 transition select-none';
        card.innerHTML =
            '<input type="checkbox" class="yt-video-cb w-4 h-4 rounded border-gray-300 text-indigo-600 focus:ring-indigo-500 cursor-pointer flex-shrink-0" ' +
                'data-index="' + i + '" onchange="updateSelectionCount()">' +
            '<div class="flex-shrink-0 w-24 h-14 rounded-md overflow-hidden bg-gray-100">' +
                (v.thumbnail
                    ? '<img src="' + escapeHtml(v.thumbnail) + '" alt="" class="w-full h-full object-cover">'
                    : '<div class="w-full h-full flex items-center justify-center"><i class="fas fa-video text-gray-400"></i></div>') +
            '</div>' +
            '<div class="flex-1 min-w-0">' +
                '<p class="text-sm font-medium text-gray-800 truncate">' + escapeHtml(v.title) + '</p>' +
                (publishedDate
                    ? '<p class="text-xs text-gray-400 mt-0.5"><i class="fas fa-calendar mr-1"></i>' + publishedDate + '</p>'
                    : '') +
            '</div>';
        list.appendChild(card);
    }

    updateSelectionCount();
    showPanel('ytSelectionPanel');
}

function toggleSelectAll(masterCheckbox) {
    var checkboxes = document.querySelectorAll('#ytVideoList .yt-video-cb');
    for (var i = 0; i < checkboxes.length; i++) {
        checkboxes[i].checked = masterCheckbox.checked;
    }
    updateSelectionCount();
}

function updateSelectionCount() {
    var checkboxes = document.querySelectorAll('#ytVideoList .yt-video-cb');
    var checked = 0;
    for (var i = 0; i < checkboxes.length; i++) {
        if (checkboxes[i].checked) checked++;
    }
    document.getElementById('ytSelectedCount').textContent = checked + ' selected';
    document.getElementById('ytExtractBtn').disabled = (checked === 0);

    // Update select-all checkbox state
    var selectAll = document.getElementById('ytSelectAll');
    if (checkboxes.length > 0) {
        selectAll.checked = (checked === checkboxes.length);
        selectAll.indeterminate = (checked > 0 && checked < checkboxes.length);
    }
}

// ── Extract selected videos ────────────────────────────────

function startExtract() {
    var checkboxes = document.querySelectorAll('#ytVideoList .yt-video-cb:checked');
    var selectedObjects = [];

    for (var i = 0; i < checkboxes.length; i++) {
        var idx = parseInt(checkboxes[i].getAttribute('data-index'), 10);
        if (searchResultVideos[idx] && searchResultVideos[idx]._raw) {
            selectedObjects.push(searchResultVideos[idx]._raw);
        }
    }

    if (selectedObjects.length === 0) {
        showToast('Please select at least one video.', 'warning');
        return;
    }

    showSyncProgressPanel();
    startSyncStream(currentSyncPageId, selectedObjects);
}

// ── SSE sync stream ────────────────────────────────────────

async function startSyncStream(pageId, videoObjects) {
    var fetchOpts = {
        method: 'POST',
        headers: { 'X-CSRFToken': csrfToken },
    };

    if (videoObjects) {
        fetchOpts.headers['Content-Type'] = 'application/json';
        fetchOpts.body = JSON.stringify({ video_objects: videoObjects });
    }

    try {
        var response = await fetch('/api/sync/' + pageId + '/stream', fetchOpts);

        if (!response.ok) {
            throw new Error('Sync request failed with status ' + response.status);
        }

        var reader = response.body.getReader();
        var decoder = new TextDecoder();
        var buffer = '';

        while (true) {
            var result = await reader.read();
            if (result.done) break;

            buffer += decoder.decode(result.value, { stream: true });

            var lines = buffer.split('\n');
            buffer = lines.pop();

            for (var i = 0; i < lines.length; i++) {
                var line = lines[i].trim();
                if (line.startsWith('data: ')) {
                    try {
                        var ev = JSON.parse(line.substring(6));
                        handleSyncEvent(ev);
                    } catch (e) {}
                }
            }
        }

        if (buffer.trim().startsWith('data: ')) {
            try {
                var ev = JSON.parse(buffer.trim().substring(6));
                handleSyncEvent(ev);
            } catch (e) {}
        }

    } catch (e) {
        handleSyncEvent({ type: 'error', error: e.message || 'Connection failed' });
    }
}

// ── Sync event handler ─────────────────────────────────────

var PLATFORM_FALLBACK_ICONS = {
    facebook: 'fa-facebook-f',
    instagram: 'fa-instagram',
    youtube: 'fa-video',
    linkedin: 'fa-linkedin-in',
    twitter: 'fa-twitter',
};

function handleSyncEvent(event) {
    var itemWord = currentSyncPlatform === 'youtube' ? 'videos' : 'posts';

    switch (event.type) {
        case 'start':
            document.getElementById('syncProgressText').textContent =
                'Found ' + event.total + ' ' + itemWord + ', syncing...';
            break;

        case 'video':
        case 'post': {
            var pct = Math.round((event.index / event.total) * 100);
            document.getElementById('syncProgressBar').style.width = pct + '%';
            document.getElementById('syncProgressPercent').textContent = pct + '%';
            document.getElementById('syncProgressText').textContent =
                'Syncing ' + (event.type === 'video' ? 'video' : 'post') +
                ' ' + event.index + ' of ' + event.total;

            var list = document.getElementById('syncVideoList');
            var card = document.createElement('div');
            card.className = 'flex items-center gap-3 p-3 rounded-lg border bg-white mb-2';

            var mediaHtml;
            if (event.thumbnail) {
                mediaHtml = '<img src="' + escapeHtml(event.thumbnail) + '" alt="" class="w-full h-full object-cover">';
            } else {
                var fallbackIcon = PLATFORM_FALLBACK_ICONS[currentSyncPlatform] || 'fa-file-alt';
                var platformColor = PLATFORM_COLORS[currentSyncPlatform] || 'text-gray-400';
                mediaHtml = '<div class="w-full h-full flex items-center justify-center"><i class="fab ' + fallbackIcon + ' text-xl ' + platformColor + '"></i></div>';
            }

            var titleHtml = event.permalink
                ? '<a href="' + escapeHtml(event.permalink) + '" target="_blank" class="text-sm font-medium text-gray-800 hover:text-indigo-600 truncate block">' + escapeHtml(event.title || '') + '</a>'
                : '<p class="text-sm font-medium text-gray-800 truncate">' + escapeHtml(event.title || '') + '</p>';

            var statsHtml = '<div class="flex items-center gap-3 mt-1 text-xs text-gray-500">';
            if (event.views) statsHtml += '<span><i class="fas fa-eye mr-1"></i>' + formatCount(event.views) + '</span>';
            statsHtml += '<span><i class="fas fa-heart mr-1"></i>' + formatCount(event.likes || 0) + '</span>';
            statsHtml += '<span><i class="fas fa-comment mr-1"></i>' + (event.comments_synced || 0) + ' new</span>';
            statsHtml += '</div>';

            var badgeHtml = event.skipped
                ? '<span class="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-500"><i class="fas fa-minus mr-1"></i>Skipped</span>'
                : '<span class="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-emerald-100 text-emerald-700"><i class="fas fa-check mr-1"></i>Synced</span>';

            card.innerHTML =
                '<div class="flex-shrink-0 w-24 h-14 rounded-md overflow-hidden bg-gray-100">' + mediaHtml + '</div>' +
                '<div class="flex-1 min-w-0">' + titleHtml + statsHtml + '</div>' +
                '<div class="flex-shrink-0">' + badgeHtml + '</div>';

            list.appendChild(card);
            list.scrollTop = list.scrollHeight;
            break;
        }

        case 'done': {
            if (syncProgressInterval) { clearInterval(syncProgressInterval); syncProgressInterval = null; }
            var doneLabel = PLATFORM_LABELS[currentSyncPlatform] || currentSyncPlatform;
            document.getElementById('syncTitle').textContent = 'Sync Complete';
            document.getElementById('syncSubtitle').textContent =
                (currentSyncPageName || 'Page') + ' — All ' + itemWord + ' and comments synced';
            document.getElementById('syncHeaderIcon').innerHTML =
                '<i class="fas fa-check-circle text-emerald-600 text-lg"></i>';
            document.getElementById('syncProgressBar').style.width = '100%';
            document.getElementById('syncProgressPercent').textContent = '100%';
            document.getElementById('syncProgressText').textContent = doneLabel + ' sync completed successfully';
            document.getElementById('syncProgressBar').className =
                'bg-emerald-500 h-2 rounded-full transition-all duration-300';

            document.getElementById('statVideos').textContent = event.posts_synced || 0;
            document.getElementById('statComments').textContent = event.comments_synced || 0;
            document.getElementById('statContacts').textContent = event.contacts_found || 0;

            // Update footer label from "Videos" to "Posts" for non-YouTube
            var statLabel = document.querySelector('#syncFooter .text-xs.text-gray-500');
            if (statLabel && currentSyncPlatform !== 'youtube') {
                statLabel.textContent = 'Posts';
            }

            document.getElementById('syncFooter').classList.remove('hidden');
            document.getElementById('syncCloseBtn').classList.remove('hidden');

            showToast(doneLabel + ': Synced ' + (event.posts_synced || 0) + ' ' + itemWord +
                ', ' + (event.comments_synced || 0) + ' comments!', 'success');
            break;
        }

        case 'error':
            if (syncProgressInterval) { clearInterval(syncProgressInterval); syncProgressInterval = null; }
            document.getElementById('syncTitle').textContent = 'Sync Failed';
            document.getElementById('syncSubtitle').textContent = event.error || 'An error occurred';
            document.getElementById('syncHeaderIcon').innerHTML =
                '<i class="fas fa-exclamation-circle text-rose-600 text-lg"></i>';
            document.getElementById('syncProgressBar').className =
                'bg-rose-500 h-2 rounded-full transition-all duration-300';
            document.getElementById('syncProgressBar').style.width = '100%';
            document.getElementById('syncCloseBtn').classList.remove('hidden');
            showToast(event.error || 'Sync failed.', 'danger');
            break;
    }
}

function closeSyncModal() {
    var modal = document.getElementById('syncModal');
    if (modal) {
        modal.classList.add('hidden');
        document.body.style.overflow = '';
    }
    window.location.reload();
}

// ── Convert UTC timestamps to local time ──────────────────
document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('.local-time').forEach(function(el) {
        var utc = el.getAttribute('data-utc');
        if (utc) {
            var date = new Date(utc);
            if (!isNaN(date.getTime())) {
                el.textContent = date.toLocaleString('en-US', {
                    month: 'short', day: 'numeric', year: 'numeric',
                    hour: '2-digit', minute: '2-digit'
                });
            }
        }
    });
});

// Dashboard — User Monitor
// Chart types: Polar Area (sentiment), Vertical Bar (platform), Bar (timeline), Radar (engagement),
//              Doughnut (response), Pie (comment share), Horizontal Bar (views)

document.addEventListener('DOMContentLoaded', () => {

    // ── Color palettes ──────────────────────────────────────────
    const SENTIMENT_COLORS = {
        positive: { bg: '#22c55e', light: 'rgba(34, 197, 94, 0.30)' },
        negative: { bg: '#ef4444', light: 'rgba(239, 68, 68, 0.30)' },
        neutral:  { bg: '#64748b', light: 'rgba(100, 116, 139, 0.25)' },
        lead:     { bg: '#3b82f6', light: 'rgba(59, 130, 246, 0.30)' },
        business: { bg: '#a855f7', light: 'rgba(168, 85, 247, 0.30)' },
        unknown:  { bg: '#cbd5e1', light: 'rgba(203, 213, 225, 0.25)' },
    };

    // Vibrant multi-color palette for timeline bars
    const TIMELINE_BAR_COLORS = [
        '#6366f1', '#8b5cf6', '#a855f7', '#d946ef',
        '#ec4899', '#f43f5e', '#f97316', '#eab308',
        '#22c55e', '#14b8a6', '#06b6d4', '#3b82f6',
    ];

    const PLATFORM_COLORS = {
        facebook:       { bg: '#1877F2', light: 'rgba(24, 119, 242, 0.15)' },
        instagram:      { bg: '#C13584', light: 'rgba(193, 53, 132, 0.15)' },
        youtube:        { bg: '#FF0000', light: 'rgba(255, 0, 0, 0.15)' },
        linkedin:       { bg: '#0A66C2', light: 'rgba(10, 102, 194, 0.15)' },
        twitter:        { bg: '#000000', light: 'rgba(0, 0, 0, 0.10)' },
        google_reviews: { bg: '#4285F4', light: 'rgba(66, 133, 244, 0.15)' },
    };

    const PLATFORM_LABELS = {
        facebook: 'Facebook', instagram: 'Instagram', youtube: 'YouTube',
        linkedin: 'LinkedIn', twitter: 'X / Twitter', google_reviews: 'Google Reviews',
    };

    const PLATFORM_ICONS = {
        facebook: 'fab fa-facebook', instagram: 'fab fa-instagram',
        youtube: 'fab fa-youtube', linkedin: 'fab fa-linkedin',
        twitter: 'fab fa-twitter', google_reviews: 'fab fa-google',
    };

    // ── Chart instances ─────────────────────────────────────────
    let sentimentChart = null;
    let platformChart = null;
    let timelineChart = null;
    let engagementChart = null;
    let responseChart = null;
    let commentShareChart = null;
    let viewsChart = null;

    // ── Global Chart.js defaults ────────────────────────────────
    Chart.defaults.font.family = "'Inter', 'Segoe UI', system-ui, sans-serif";
    Chart.defaults.font.size = 12;
    Chart.defaults.color = '#6b7280';
    Chart.defaults.animation.duration = 700;
    Chart.defaults.animation.easing = 'easeOutQuart';

    const tooltipConfig = {
        backgroundColor: 'rgba(15, 23, 42, 0.9)',
        titleFont: { size: 13, weight: '600' },
        bodyFont: { size: 12 },
        padding: 12,
        cornerRadius: 8,
        displayColors: true,
        boxPadding: 4,
    };

    // ── DOM references ──────────────────────────────────────────
    const filterPlatform = document.getElementById('filterPlatform');
    const filterChannel = document.getElementById('filterChannel');
    const filterPost = document.getElementById('filterPost');
    const filterDateFrom = document.getElementById('filterDateFrom');
    const filterDateTo = document.getElementById('filterDateTo');
    const channelWrapper = document.getElementById('channelFilterWrapper');
    const postWrapper = document.getElementById('postFilterWrapper');
    const channelLabel = document.getElementById('channelFilterLabel');
    const postLabel = document.getElementById('postFilterLabel');
    const clearBtn = document.getElementById('clearFilters');

    // ── Platform icon buttons ────────────────────────────────────
    const platformBtns = document.querySelectorAll('.platform-icon-btn');

    function updatePlatformBtnStyles() {
        const activePlatform = filterPlatform.value;
        platformBtns.forEach(btn => {
            const p = btn.dataset.platform;
            const color = btn.dataset.color;
            const isActive = p === activePlatform;

            if (isActive) {
                if (p === '') {
                    btn.style.backgroundColor = '#6366f1';
                    btn.style.borderColor = '#6366f1';
                    btn.style.color = '#ffffff';
                } else {
                    btn.style.backgroundColor = color;
                    btn.style.borderColor = color;
                    btn.style.color = '#ffffff';
                }
                btn.style.transform = 'scale(1.05)';
                btn.style.boxShadow = `0 2px 8px ${color || '#6366f1'}40`;
            } else {
                btn.style.backgroundColor = '#ffffff';
                btn.style.borderColor = '#e5e7eb';
                btn.style.color = p === '' ? '#6b7280' : (color || '#6b7280');
                btn.style.transform = 'scale(1)';
                btn.style.boxShadow = 'none';
            }
        });
    }

    platformBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            filterPlatform.value = btn.dataset.platform;
            updatePlatformBtnStyles();
            filterPlatform.dispatchEvent(new Event('change'));
        });

        btn.addEventListener('mouseenter', () => {
            if (btn.dataset.platform !== filterPlatform.value) {
                const color = btn.dataset.color;
                btn.style.borderColor = color || '#a5b4fc';
                btn.style.backgroundColor = (color || '#6366f1') + '12';
            }
        });
        btn.addEventListener('mouseleave', () => {
            if (btn.dataset.platform !== filterPlatform.value) {
                btn.style.borderColor = '#e5e7eb';
                btn.style.backgroundColor = '#ffffff';
            }
        });
    });

    clearBtn.addEventListener('click', () => updatePlatformBtnStyles());
    updatePlatformBtnStyles();

    // ── Helpers ──────────────────────────────────────────────────
    function showEl(id) {
        const el = document.getElementById(id);
        if (el) el.style.display = '';
    }
    function hideEl(id) {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
    }
    function showChart(wrapperId, skeletonId, emptyId) {
        showEl(wrapperId);
        hideEl(skeletonId);
        hideEl(emptyId);
    }
    function showEmpty(wrapperId, skeletonId, emptyId) {
        hideEl(wrapperId);
        hideEl(skeletonId);
        showEl(emptyId);
    }
    function showSkeleton(wrapperId, skeletonId, emptyId) {
        hideEl(wrapperId);
        showEl(skeletonId);
        hideEl(emptyId);
    }
    function escapeHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }
    function capitalize(str) {
        if (!str) return '';
        return str.charAt(0).toUpperCase() + str.slice(1);
    }
    function formatNumber(n) {
        if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
        if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
        return (n || 0).toLocaleString();
    }
    function formatDate(dateStr) {
        const d = new Date(dateStr + 'T00:00:00');
        return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    }

    // ── Filter helpers ───────────────────────────────────────────
    function getFilterParams() {
        const params = new URLSearchParams();
        if (filterPlatform.value) params.set('platform', filterPlatform.value);
        if (filterChannel.value) params.set('page_id', filterChannel.value);
        if (filterPost.value) params.set('post_id', filterPost.value);
        if (filterDateFrom.value) params.set('date_from', filterDateFrom.value);
        if (filterDateTo.value) params.set('date_to', filterDateTo.value);
        return params.toString();
    }

    const PLATFORM_CHANNEL_LABELS = {
        youtube: { channel: 'Channel', post: 'Video' },
        facebook: { channel: 'Page', post: 'Post' },
        instagram: { channel: 'Account', post: 'Post' },
        linkedin: { channel: 'Page', post: 'Post' },
        twitter: { channel: 'Account', post: 'Post' },
        google_reviews: { channel: 'Location', post: 'Review' },
    };

    function updateClearButton() {
        const hasFilter = filterPlatform.value || filterChannel.value || filterPost.value || filterDateFrom.value || filterDateTo.value;
        clearBtn.style.display = hasFilter ? '' : 'none';
    }

    // ── Filter event listeners ───────────────────────────────────
    filterPlatform.addEventListener('change', async () => {
        const platform = filterPlatform.value;
        filterChannel.innerHTML = '<option value="">All Channels</option>';
        filterPost.innerHTML = '<option value="">All Posts</option>';
        postWrapper.style.display = 'none';

        if (platform) {
            const labels = PLATFORM_CHANNEL_LABELS[platform] || { channel: 'Channel', post: 'Post' };
            channelLabel.textContent = labels.channel;
            postLabel.textContent = labels.post;
            filterChannel.querySelector('option').textContent = `All ${labels.channel}s`;
            filterPost.querySelector('option').textContent = `All ${labels.post}s`;
            channelWrapper.style.display = '';

            try {
                const res = await fetch(`/api/pages/by-platform?platform=${platform}`);
                const data = await res.json();
                data.pages.forEach(page => {
                    const opt = document.createElement('option');
                    opt.value = page.id;
                    opt.textContent = page.page_name;
                    filterChannel.appendChild(opt);
                });
            } catch (e) {
                console.error('Failed to load channels:', e);
            }
        } else {
            channelWrapper.style.display = 'none';
        }
        updateClearButton();
        loadDashboardData();
        if (typeof loadKeywords === 'function') loadKeywords();
    });

    filterChannel.addEventListener('change', async () => {
        const pageId = filterChannel.value;
        const platform = filterPlatform.value;
        const labels = PLATFORM_CHANNEL_LABELS[platform] || { channel: 'Channel', post: 'Post' };
        filterPost.innerHTML = `<option value="">All ${labels.post}s</option>`;

        if (pageId) {
            postWrapper.style.display = '';
            try {
                const res = await fetch(`/api/posts/by-page?page_id=${pageId}`);
                const data = await res.json();
                data.posts.forEach(post => {
                    const opt = document.createElement('option');
                    opt.value = post.id;
                    opt.textContent = post.caption;
                    filterPost.appendChild(opt);
                });
            } catch (e) {
                console.error('Failed to load posts:', e);
            }
        } else {
            postWrapper.style.display = 'none';
        }
        updateClearButton();
        loadDashboardData();
        if (typeof loadKeywords === 'function') loadKeywords();
    });

    filterPost.addEventListener('change', () => {
        updateClearButton();
        loadDashboardData();
        if (typeof loadKeywords === 'function') loadKeywords();
    });

    filterDateFrom.addEventListener('change', () => {
        updateClearButton();
        loadDashboardData();
        if (typeof loadKeywords === 'function') loadKeywords();
    });

    filterDateTo.addEventListener('change', () => {
        updateClearButton();
        loadDashboardData();
        if (typeof loadKeywords === 'function') loadKeywords();
    });

    clearBtn.addEventListener('click', () => {
        filterPlatform.value = '';
        filterChannel.innerHTML = '<option value="">All Channels</option>';
        filterPost.innerHTML = '<option value="">All Posts</option>';
        filterDateFrom.value = '';
        filterDateTo.value = '';
        channelWrapper.style.display = 'none';
        postWrapper.style.display = 'none';
        updateClearButton();
        updatePlatformBtnStyles();
        loadDashboardData();
        if (typeof loadKeywords === 'function') loadKeywords();
    });

    // ═══════════════════════════════════════════════════════════════
    //  RENDER ALL CHARTS from a data object
    // ═══════════════════════════════════════════════════════════════
    function renderAllCharts(data) {
        updateKPIs(data.kpi, data.engagement);
        renderSentimentChart(data.sentiment);
        renderPlatformChart(data.platforms);
        renderTimelineChart(data.timeline);
        renderEngagementChart(data.engagement);
        renderResponseChart(data.kpi);
        renderCommentShareChart(data.comment_platforms);
        renderViewsChart(data.engagement);
        renderTopPosts(data.top_posts);
        renderRecentComments(data.recent_comments);
        renderRecentContacts(data.recent_contacts);
    }

    // ═══════════════════════════════════════════════════════════════
    //  LOAD DASHBOARD DATA (API fetch — only used for filter changes)
    // ═══════════════════════════════════════════════════════════════
    async function loadDashboardData() {
        showSkeleton('sentimentChartWrapper', 'sentimentSkeleton', 'sentimentEmpty');
        showSkeleton('platformChartWrapper', 'platformSkeleton', 'platformEmpty');
        showSkeleton('timelineChartWrapper', 'timelineSkeleton', 'timelineEmpty');
        showSkeleton('engagementChartWrapper', 'engagementSkeleton', 'engagementEmpty');
        showSkeleton('responseChartWrapper', 'responseSkeleton', 'responseEmpty');
        showSkeleton('commentShareChartWrapper', 'commentShareSkeleton', 'commentShareEmpty');
        showSkeleton('viewsChartWrapper', 'viewsSkeleton', 'viewsEmpty');

        try {
            const qs = getFilterParams();
            const res = await fetch(`/api/dashboard/stats${qs ? '?' + qs : ''}`);
            const data = await res.json();
            renderAllCharts(data);
        } catch (e) {
            console.error('Failed to load dashboard stats:', e);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  KPI CARDS (animated counting)
    // ═══════════════════════════════════════════════════════════════
    function animateValue(el, target) {
        const start = parseInt(el.textContent.replace(/,/g, '')) || 0;
        if (start === target) return;
        const duration = 600;
        const startTime = performance.now();

        function update(now) {
            const elapsed = now - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            const current = Math.round(start + (target - start) * eased);
            el.textContent = current.toLocaleString();
            if (progress < 1) requestAnimationFrame(update);
        }
        requestAnimationFrame(update);
    }

    function updateKPIs(kpi, engagement) {
        if (!kpi) return;
        const pairs = [
            ['kpiPosts', kpi.total_posts],
            ['kpiComments', kpi.total_comments],
            ['kpiLikes', kpi.total_likes],
            ['kpiShares', kpi.total_shares],
            ['kpiUnreplied', kpi.unreplied_comments],
            ['kpiUnreplied2', kpi.unreplied_comments],
            ['kpiContacts', kpi.total_contacts],
        ];
        pairs.forEach(([id, val]) => {
            const el = document.getElementById(id);
            if (el) animateValue(el, val || 0);
        });

        const viewsEls = ['kpiViews', 'kpiViews2'];
        if (engagement) {
            const totalViews = Object.values(engagement).reduce((sum, e) => sum + (e.views || 0), 0);
            viewsEls.forEach(id => {
                const el = document.getElementById(id);
                if (el) animateValue(el, totalViews);
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  RESPONSE TRACKER — DOUGHNUT CHART (replied vs unreplied)
    // ═══════════════════════════════════════════════════════════════
    function renderResponseChart(kpi) {
        const canvas = document.getElementById('responseChart');
        if (!canvas) return;
        if (responseChart) responseChart.destroy();

        const total = kpi ? (kpi.total_comments || 0) : 0;
        const unreplied = kpi ? (kpi.unreplied_comments || 0) : 0;
        const replied = total - unreplied;

        const totalEl = document.getElementById('responseTotal');
        if (totalEl) totalEl.textContent = total ? `${total.toLocaleString()} comments` : '';

        if (total === 0) {
            showEmpty('responseChartWrapper', 'responseSkeleton', 'responseEmpty');
            return;
        }

        responseChart = new Chart(canvas, {
            type: 'doughnut',
            data: {
                labels: ['Replied', 'Unreplied'],
                datasets: [{
                    data: [replied, unreplied],
                    backgroundColor: ['#22c55e', '#f59e0b'],
                    hoverBackgroundColor: ['#16a34a', '#d97706'],
                    borderWidth: 0,
                    spacing: 2,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '65%',
                layout: { padding: 4 },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 12,
                            usePointStyle: true,
                            pointStyle: 'circle',
                            font: { size: 11, weight: '500' },
                        },
                    },
                    tooltip: {
                        ...tooltipConfig,
                        callbacks: {
                            label(ctx) {
                                const val = ctx.raw;
                                const pct = ((val / total) * 100).toFixed(1);
                                return ` ${ctx.label}: ${val.toLocaleString()} (${pct}%)`;
                            },
                        },
                    },
                },
            },
            plugins: [{
                id: 'centerText',
                afterDraw(chart) {
                    const { ctx, width, height } = chart;
                    const pct = total > 0 ? Math.round((replied / total) * 100) : 0;
                    ctx.save();
                    ctx.font = 'bold 22px Inter, Segoe UI, sans-serif';
                    ctx.fillStyle = '#16a34a';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    const centerY = (chart.chartArea.top + chart.chartArea.bottom) / 2;
                    ctx.fillText(pct + '%', width / 2, centerY - 8);
                    ctx.font = '11px Inter, Segoe UI, sans-serif';
                    ctx.fillStyle = '#6b7280';
                    ctx.fillText('replied', width / 2, centerY + 14);
                    ctx.restore();
                },
            }],
        });
        showChart('responseChartWrapper', 'responseSkeleton', 'responseEmpty');
    }

    // ═══════════════════════════════════════════════════════════════
    //  COMMENT SHARE BY PLATFORM — PIE CHART
    // ═══════════════════════════════════════════════════════════════
    function renderCommentShareChart(commentPlatforms) {
        const canvas = document.getElementById('commentShareChart');
        if (!canvas) return;
        if (commentShareChart) commentShareChart.destroy();

        const entries = Object.entries(commentPlatforms || {}).filter(([, count]) => count > 0);
        const total = entries.reduce((sum, [, count]) => sum + count, 0);

        const totalEl = document.getElementById('commentShareTotal');
        if (totalEl) totalEl.textContent = total ? `${total.toLocaleString()} comments` : '';

        if (entries.length === 0) {
            showEmpty('commentShareChartWrapper', 'commentShareSkeleton', 'commentShareEmpty');
            return;
        }

        entries.sort((a, b) => b[1] - a[1]);
        const labels = entries.map(([p]) => PLATFORM_LABELS[p] || capitalize(p));
        const values = entries.map(([, count]) => count);
        const colors = entries.map(([p]) => (PLATFORM_COLORS[p] || { bg: '#6366f1' }).bg);
        const lightColors = entries.map(([p]) => (PLATFORM_COLORS[p] || { light: 'rgba(13,148,136,0.15)' }).light);

        commentShareChart = new Chart(canvas, {
            type: 'pie',
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: colors,
                    hoverBackgroundColor: colors,
                    borderColor: '#ffffff',
                    borderWidth: 2,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: 4 },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 10,
                            usePointStyle: true,
                            pointStyle: 'circle',
                            font: { size: 10, weight: '500' },
                        },
                    },
                    tooltip: {
                        ...tooltipConfig,
                        callbacks: {
                            label(ctx) {
                                const val = ctx.raw;
                                const pct = ((val / total) * 100).toFixed(1);
                                return ` ${ctx.label}: ${val.toLocaleString()} comments (${pct}%)`;
                            },
                        },
                    },
                },
            },
        });
        showChart('commentShareChartWrapper', 'commentShareSkeleton', 'commentShareEmpty');
    }

    // ═══════════════════════════════════════════════════════════════
    //  VIEWS BY CHANNEL — HORIZONTAL BAR CHART
    // ═══════════════════════════════════════════════════════════════
    function renderViewsChart(engagement) {
        const canvas = document.getElementById('viewsChart');
        if (!canvas) return;
        if (viewsChart) viewsChart.destroy();

        // Show ALL platforms (even those with 0 views) so the chart is never empty
        const entries = Object.entries(engagement || {});
        const totalViews = entries.reduce((sum, [, e]) => sum + (e.views || 0), 0);

        const totalEl = document.getElementById('viewsBreakdownTotal');
        if (totalEl) totalEl.textContent = totalViews ? `${formatNumber(totalViews)} total` : '';

        if (entries.length === 0) {
            showEmpty('viewsChartWrapper', 'viewsSkeleton', 'viewsEmpty');
            return;
        }

        entries.sort((a, b) => (b[1].views || 0) - (a[1].views || 0));
        const labels = entries.map(([p]) => PLATFORM_LABELS[p] || capitalize(p));
        const values = entries.map(([, e]) => e.views || 0);
        const colors = entries.map(([p]) => (PLATFORM_COLORS[p] || { bg: '#6366f1' }).bg);

        viewsChart = new Chart(canvas, {
            type: 'doughnut',
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: colors,
                    hoverBackgroundColor: colors,
                    borderColor: '#ffffff',
                    borderWidth: 2,
                    spacing: 2,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '60%',
                layout: { padding: 4 },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 10,
                            usePointStyle: true,
                            pointStyle: 'circle',
                            font: { size: 10, weight: '500' },
                        },
                    },
                    tooltip: {
                        ...tooltipConfig,
                        callbacks: {
                            label(ctx) {
                                const pct = totalViews > 0 ? ((ctx.raw / totalViews) * 100).toFixed(1) : '0.0';
                                return ` ${ctx.label}: ${formatNumber(ctx.raw)} views (${pct}%)`;
                            },
                        },
                    },
                },
            },
            plugins: [{
                id: 'viewsCenterText',
                afterDraw(chart) {
                    const { ctx, width } = chart;
                    ctx.save();
                    ctx.font = 'bold 20px Inter, Segoe UI, sans-serif';
                    ctx.fillStyle = '#374151';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    const centerY = (chart.chartArea.top + chart.chartArea.bottom) / 2;
                    ctx.fillText(formatNumber(totalViews), width / 2, centerY - 8);
                    ctx.font = '11px Inter, Segoe UI, sans-serif';
                    ctx.fillStyle = '#6b7280';
                    ctx.fillText('views', width / 2, centerY + 14);
                    ctx.restore();
                },
            }],
        });
        showChart('viewsChartWrapper', 'viewsSkeleton', 'viewsEmpty');
    }

    // ═══════════════════════════════════════════════════════════════
    //  SENTIMENT — POLAR AREA CHART
    // ═══════════════════════════════════════════════════════════════
    function renderSentimentChart(sentiment) {
        const canvas = document.getElementById('sentimentChart');
        if (!canvas) return;
        if (sentimentChart) sentimentChart.destroy();

        const labels = Object.keys(sentiment || {});
        const values = Object.values(sentiment || {});
        const total = values.reduce((a, b) => a + b, 0);

        const totalEl = document.getElementById('sentimentTotal');
        if (totalEl) totalEl.textContent = total ? `${total.toLocaleString()} total` : '';

        if (labels.length === 0 || total === 0) {
            showEmpty('sentimentChartWrapper', 'sentimentSkeleton', 'sentimentEmpty');
            return;
        }

        const colors = labels.map(l => (SENTIMENT_COLORS[l] || SENTIMENT_COLORS.unknown).bg);
        const lightColors = labels.map(l => (SENTIMENT_COLORS[l] || SENTIMENT_COLORS.unknown).light);

        // Ensure small segments (like Lead) are visible in polar area chart:
        // floor each value at 4% of the max so tiny segments still render.
        // Real values are kept for tooltips/legend via the `realValues` array.
        const realValues = [...values];
        const maxVal = Math.max(...values);
        const minVisible = maxVal * 0.04;
        const displayValues = values.map(v => Math.max(v, minVisible));

        sentimentChart = new Chart(canvas, {
            type: 'polarArea',
            data: {
                labels: labels.map(l => capitalize(l)),
                datasets: [{
                    data: displayValues,
                    backgroundColor: lightColors,
                    borderColor: colors,
                    borderWidth: 2,
                    hoverBackgroundColor: colors.map(c => c + '40'),
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: 4 },
                scales: {
                    r: {
                        display: false,
                    },
                },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 12,
                            usePointStyle: true,
                            pointStyle: 'rectRounded',
                            font: { size: 11, weight: '500' },
                            generateLabels(chart) {
                                const data = chart.data;
                                return data.labels.map((label, i) => ({
                                    text: `${label} (${realValues[i].toLocaleString()})`,
                                    fillStyle: data.datasets[0].borderColor[i],
                                    strokeStyle: 'transparent',
                                    pointStyle: 'rectRounded',
                                    index: i,
                                    hidden: false,
                                }));
                            },
                        },
                    },
                    tooltip: {
                        ...tooltipConfig,
                        callbacks: {
                            label(ctx) {
                                const val = realValues[ctx.dataIndex];
                                const pct = ((val / total) * 100).toFixed(1);
                                return ` ${ctx.label}: ${val.toLocaleString()} (${pct}%)`;
                            },
                        },
                    },
                },
            },
        });
        showChart('sentimentChartWrapper', 'sentimentSkeleton', 'sentimentEmpty');
    }

    // ═══════════════════════════════════════════════════════════════
    //  PLATFORM — VERTICAL BAR CHART
    // ═══════════════════════════════════════════════════════════════
    function renderPlatformChart(platforms) {
        const canvas = document.getElementById('platformChart');
        if (!canvas) return;
        if (platformChart) platformChart.destroy();

        const entries = Object.entries(platforms || {});
        const total = entries.reduce((sum, [, c]) => sum + c, 0);

        const totalEl = document.getElementById('platformTotal');
        if (totalEl) totalEl.textContent = total ? `${total.toLocaleString()} posts` : '';

        if (entries.length === 0) {
            showEmpty('platformChartWrapper', 'platformSkeleton', 'platformEmpty');
            return;
        }

        entries.sort((a, b) => b[1] - a[1]);
        const labels = entries.map(([p]) => PLATFORM_LABELS[p] || capitalize(p));
        const values = entries.map(([, c]) => c);
        const colors = entries.map(([p]) => (PLATFORM_COLORS[p] || { bg: '#6366f1' }).bg);

        platformChart = new Chart(canvas, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: 'Posts',
                    data: values,
                    backgroundColor: colors,
                    hoverBackgroundColor: colors,
                    borderRadius: 8,
                    borderSkipped: false,
                    maxBarThickness: 40,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { top: 8 } },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        ...tooltipConfig,
                        callbacks: {
                            label(ctx) {
                                const pct = ((ctx.raw / total) * 100).toFixed(1);
                                return ` ${ctx.raw.toLocaleString()} posts (${pct}%)`;
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: { font: { size: 10, weight: '500' }, color: '#374151' },
                    },
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(0,0,0,0.04)', drawBorder: false },
                        ticks: { stepSize: 1, font: { size: 10 } },
                    },
                },
            },
        });
        showChart('platformChartWrapper', 'platformSkeleton', 'platformEmpty');
    }

    // ═══════════════════════════════════════════════════════════════
    //  COMMENT ACTIVITY — BAR CHART (full width)
    // ═══════════════════════════════════════════════════════════════
    function renderTimelineChart(timeline) {
        const canvas = document.getElementById('timelineChart');
        if (!canvas) return;
        if (timelineChart) timelineChart.destroy();

        const dates = Object.keys(timeline || {}).sort();
        const counts = dates.map(d => timeline[d]);

        const rangeEl = document.getElementById('timelineRange');
        if (rangeEl && dates.length >= 2) {
            rangeEl.textContent = `${formatDate(dates[0])} — ${formatDate(dates[dates.length - 1])}`;
        } else if (rangeEl) {
            rangeEl.textContent = '';
        }

        if (dates.length === 0) {
            showEmpty('timelineChartWrapper', 'timelineSkeleton', 'timelineEmpty');
            return;
        }

        // Assign each bar a different color from the vibrant palette
        const barColors = counts.map((_, i) => TIMELINE_BAR_COLORS[i % TIMELINE_BAR_COLORS.length]);
        const barHoverColors = barColors.map(c => c + 'cc');

        timelineChart = new Chart(canvas, {
            type: 'bar',
            data: {
                labels: dates.map(d => formatDate(d)),
                datasets: [{
                    label: 'Comments',
                    data: counts,
                    backgroundColor: barColors,
                    hoverBackgroundColor: barHoverColors,
                    borderRadius: 6,
                    borderSkipped: false,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        ...tooltipConfig,
                        callbacks: {
                            title(items) { return items[0]?.label || ''; },
                            label(ctx) { return ` ${ctx.raw.toLocaleString()} comments`; },
                        },
                    },
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: {
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 12,
                            font: { size: 11 },
                        },
                    },
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(0,0,0,0.04)', drawBorder: false },
                        ticks: { stepSize: 1, font: { size: 11 } },
                    },
                },
            },
        });
        showChart('timelineChartWrapper', 'timelineSkeleton', 'timelineEmpty');
    }

    // ═══════════════════════════════════════════════════════════════
    //  ENGAGEMENT — RADAR CHART
    // ═══════════════════════════════════════════════════════════════
    function renderEngagementChart(engagement) {
        const canvas = document.getElementById('engagementChart');
        if (!canvas) return;
        if (engagementChart) engagementChart.destroy();

        const entries = Object.entries(engagement || {});
        if (entries.length === 0) {
            showEmpty('engagementChartWrapper', 'engagementSkeleton', 'engagementEmpty');
            return;
        }

        const labels = entries.map(([p]) => PLATFORM_LABELS[p] || capitalize(p));

        engagementChart = new Chart(canvas, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Likes',
                        data: entries.map(([, e]) => e.likes),
                        backgroundColor: '#ef4444',
                        hoverBackgroundColor: '#dc2626',
                        borderRadius: 4,
                        borderSkipped: false,
                    },
                    {
                        label: 'Comments',
                        data: entries.map(([, e]) => e.comments),
                        backgroundColor: '#3b82f6',
                        hoverBackgroundColor: '#2563eb',
                        borderRadius: 4,
                        borderSkipped: false,
                    },
                    {
                        label: 'Shares',
                        data: entries.map(([, e]) => e.shares),
                        backgroundColor: '#eab308',
                        hoverBackgroundColor: '#ca8a04',
                        borderRadius: 4,
                        borderSkipped: false,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        ...tooltipConfig,
                        callbacks: {
                            label(ctx) {
                                return ` ${ctx.dataset.label}: ${formatNumber(ctx.raw)}`;
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: { font: { size: 10, weight: '500' }, color: '#374151' },
                    },
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(0,0,0,0.04)', drawBorder: false },
                        ticks: {
                            font: { size: 10 },
                            callback(val) { return formatNumber(val); },
                        },
                    },
                },
            },
        });
        showChart('engagementChartWrapper', 'engagementSkeleton', 'engagementEmpty');
    }

    // ═══════════════════════════════════════════════════════════════
    //  TOP PERFORMING POSTS
    // ═══════════════════════════════════════════════════════════════
    function renderTopPosts(posts) {
        const container = document.getElementById('topPostsContainer');
        if (!container) return;

        if (!posts || posts.length === 0) {
            container.innerHTML = `
                <div class="text-center py-10 text-gray-400">
                    <i class="fas fa-trophy text-3xl mb-3"></i>
                    <p class="text-sm">No posts to rank yet</p>
                </div>`;
            return;
        }

        container.innerHTML = posts.map((p, i) => {
            const engagement = (p.likes + p.comments + p.shares);
            const platformColor = (PLATFORM_COLORS[p.platform] || { bg: '#6366f1' }).bg;
            const icon = PLATFORM_ICONS[p.platform] || 'fas fa-globe';
            const rankColors = ['text-amber-500', 'text-gray-400', 'text-amber-700', 'text-gray-400', 'text-gray-400'];

            return `
                <a href="/posts/${p.id}" class="flex items-center gap-3 p-3 rounded-lg hover:bg-gray-50 transition group">
                    <div class="flex-shrink-0 w-7 text-center">
                        <span class="text-sm font-bold ${rankColors[i] || 'text-gray-400'}">#${i + 1}</span>
                    </div>
                    ${p.thumbnail
                        ? `<img src="${escapeHtml(p.thumbnail)}" alt="" class="w-10 h-10 rounded-lg object-cover flex-shrink-0 bg-gray-100">`
                        : `<div class="w-10 h-10 rounded-lg bg-gray-100 flex items-center justify-center flex-shrink-0"><i class="fas fa-image text-gray-300 text-xs"></i></div>`
                    }
                    <div class="flex-1 min-w-0">
                        <p class="text-sm text-gray-800 truncate group-hover:text-indigo-600 transition">${escapeHtml(p.caption)}</p>
                        <div class="flex items-center gap-3 mt-0.5">
                            <span class="text-xs" style="color:${platformColor}"><i class="${icon}"></i></span>
                            <span class="text-xs text-gray-400"><i class="fas fa-heart text-rose-300 mr-0.5"></i>${formatNumber(p.likes)}</span>
                            <span class="text-xs text-gray-400"><i class="fas fa-comment text-indigo-300 mr-0.5"></i>${formatNumber(p.comments)}</span>
                            <span class="text-xs text-gray-400"><i class="fas fa-share text-amber-300 mr-0.5"></i>${formatNumber(p.shares)}</span>
                        </div>
                    </div>
                    <div class="flex-shrink-0 text-right">
                        <p class="text-sm font-semibold text-gray-700">${formatNumber(engagement)}</p>
                        <p class="text-xs text-gray-400">total</p>
                    </div>
                </a>
            `;
        }).join('<div class="border-t border-gray-100"></div>');
    }

    // ═══════════════════════════════════════════════════════════════
    //  RECENT COMMENTS
    // ═══════════════════════════════════════════════════════════════
    function renderRecentComments(comments) {
        const container = document.getElementById('recentCommentsContainer');
        if (!container) return;

        if (!comments || comments.length === 0) {
            container.innerHTML = `
                <div class="text-center py-6 text-gray-400">
                    <i class="fas fa-comment-slash text-2xl mb-2"></i>
                    <p class="text-sm">No recent comments</p>
                </div>`;
            return;
        }

        container.innerHTML = comments.map(c => {
            const initial = (c.author_name || '?').charAt(0).toUpperCase();
            return `
            <div class="flex items-start gap-3 p-3 rounded-lg hover:bg-gray-50 transition">
                <div class="w-8 h-8 rounded-full bg-indigo-100 flex items-center justify-center flex-shrink-0 mt-0.5">
                    <span class="text-indigo-600 text-xs font-bold">${initial}</span>
                </div>
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-2 mb-1">
                        <p class="text-sm font-medium text-gray-800 truncate">${escapeHtml(c.author_name)}</p>
                        <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-${c.sentiment_color}-100 text-${c.sentiment_color}-700">
                            ${escapeHtml(c.sentiment)}
                        </span>
                    </div>
                    <p class="text-xs text-gray-500 truncate">${escapeHtml(c.comment_text)}</p>
                    <div class="flex items-center gap-2 mt-1">
                        <span class="text-xs text-gray-400">
                            <i class="fab fa-${c.platform} mr-1"></i>${escapeHtml(c.platform)}
                        </span>
                    </div>
                </div>
            </div>`;
        }).join('');
    }

    // ═══════════════════════════════════════════════════════════════
    //  RECENT CONTACTS
    // ═══════════════════════════════════════════════════════════════
    function renderRecentContacts(contacts) {
        const container = document.getElementById('recentContactsContainer');
        if (!container) return;

        if (!contacts || contacts.length === 0) {
            container.innerHTML = `
                <div class="text-center py-6 text-gray-400">
                    <i class="fas fa-address-book text-2xl mb-2"></i>
                    <p class="text-sm">No recent contacts</p>
                </div>`;
            return;
        }

        const typeClasses = {
            lead: 'bg-indigo-100 text-indigo-700',
            customer: 'bg-emerald-100 text-emerald-700',
        };

        container.innerHTML = contacts.map(ct => {
            const cls = typeClasses[ct.contact_type] || 'bg-gray-100 text-gray-700';
            let contactInfo = '<span class="text-gray-400">No contact info</span>';
            if (ct.email) contactInfo = `<i class="fas fa-envelope mr-1"></i>${escapeHtml(ct.email)}`;
            else if (ct.phone) contactInfo = `<i class="fas fa-phone mr-1"></i>${escapeHtml(ct.phone)}`;

            return `
                <div class="flex items-start gap-3 p-3 rounded-lg hover:bg-gray-50 transition">
                    <div class="w-8 h-8 rounded-full bg-emerald-100 flex items-center justify-center flex-shrink-0 mt-0.5">
                        <i class="fas fa-address-book text-emerald-500 text-xs"></i>
                    </div>
                    <div class="flex-1 min-w-0">
                        <div class="flex items-center gap-2 mb-1">
                            <p class="text-sm font-medium text-gray-800 truncate">${escapeHtml(ct.name)}</p>
                            <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${cls}">
                                ${capitalize(ct.contact_type || 'contact')}
                            </span>
                        </div>
                        <p class="text-xs text-gray-500 truncate">${contactInfo}</p>
                    </div>
                </div>
            `;
        }).join('');
    }

    // ═══════════════════════════════════════════════════════════════
    //  TRENDING KEYWORDS (teal/emerald gradient chips)
    // ═══════════════════════════════════════════════════════════════
    const KEYWORD_COLORS = [
        { bg: 'bg-rose-500',    text: 'text-white',        size: 'text-base', px: 'px-4 py-1.5' },
        { bg: 'bg-blue-500',    text: 'text-white',        size: 'text-sm',   px: 'px-3.5 py-1.5' },
        { bg: 'bg-emerald-500', text: 'text-white',        size: 'text-sm',   px: 'px-3 py-1' },
        { bg: 'bg-amber-400',   text: 'text-amber-900',    size: 'text-sm',   px: 'px-3 py-1' },
        { bg: 'bg-purple-200',  text: 'text-purple-700',   size: 'text-xs',   px: 'px-2.5 py-1' },
    ];

    function getKeywordStyle(score) {
        if (score >= 0.8) return KEYWORD_COLORS[0];
        if (score >= 0.6) return KEYWORD_COLORS[1];
        if (score >= 0.4) return KEYWORD_COLORS[2];
        if (score >= 0.2) return KEYWORD_COLORS[3];
        return KEYWORD_COLORS[4];
    }

    function renderKeywords(keywords, totalComments) {
        const container = document.getElementById('keywordsContainer');
        const skeleton = document.getElementById('keywordsSkeleton');
        const empty = document.getElementById('keywordsEmpty');
        const totalEl = document.getElementById('keywordsTotal');

        if (!container) return;

        if (skeleton) skeleton.style.display = 'none';

        if (totalEl) {
            totalEl.textContent = totalComments ? `from ${totalComments.toLocaleString()} comments` : '';
        }

        if (!keywords || keywords.length === 0) {
            container.style.display = 'none';
            if (empty) { empty.style.display = ''; empty.classList.remove('hidden'); }
            return;
        }

        if (empty) { empty.style.display = 'none'; }
        container.style.display = '';

        container.innerHTML = `
            <div class="flex flex-wrap gap-2">
                ${keywords.map(k => {
                    const s = getKeywordStyle(k.score);
                    const pct = Math.round(k.score * 100);
                    return `<span class="inline-flex items-center gap-1.5 ${s.px} ${s.bg} ${s.text} ${s.size} font-medium rounded-full cursor-default transition hover:opacity-90 hover:scale-105"
                                  title="Relevance: ${pct}%">
                                ${escapeHtml(k.keyword)}
                                <span class="opacity-70 ${s.size}" style="font-size:0.65em">${pct}%</span>
                            </span>`;
                }).join('')}
            </div>
        `;
    }

    async function loadKeywords() {
        const skeleton = document.getElementById('keywordsSkeleton');
        const container = document.getElementById('keywordsContainer');
        const empty = document.getElementById('keywordsEmpty');

        if (skeleton) skeleton.style.display = '';
        if (container) container.style.display = 'none';
        if (empty) { empty.style.display = 'none'; empty.classList.add('hidden'); }

        try {
            const qs = getFilterParams();
            const res = await fetch(`/api/dashboard/keywords${qs ? '?' + qs : ''}`);
            const data = await res.json();
            renderKeywords(data.keywords, data.total_comments);
        } catch (e) {
            console.error('Failed to load keywords:', e);
            if (skeleton) skeleton.style.display = 'none';
        }
    }

    // ── Initial load ────────────────────────────────────────────
    // Use server-embedded data for instant chart rendering (no API call)
    if (window.__DASHBOARD_DATA__) {
        const data = window.__DASHBOARD_DATA__;
        renderAllCharts(data);
        // Render keywords instantly from embedded data
        if (data.keywords !== undefined) {
            renderKeywords(data.keywords, data.keywords_total || 0);
        } else {
            loadKeywords();
        }
        delete window.__DASHBOARD_DATA__;
    } else {
        loadDashboardData();
        loadKeywords();
    }
});

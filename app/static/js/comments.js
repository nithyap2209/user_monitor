// Comment actions with button loading states

function toggleReplyForm(commentId) {
    const form = document.getElementById(`reply-form-${commentId}`);
    if (!form) return;
    const isHidden = form.classList.contains('hidden');
    form.classList.toggle('hidden');
    if (isHidden) {
        const ta = document.getElementById(`reply-text-${commentId}`);
        if (ta) { ta.value = ''; ta.focus(); }
    }
}

async function sendInlineReply(commentId) {
    const ta = document.getElementById(`reply-text-${commentId}`);
    const sendBtn = document.getElementById(`reply-send-${commentId}`);
    if (!ta) return;

    const text = ta.value.trim();
    if (!text) {
        ta.style.borderColor = '#f87171';
        ta.focus();
        return;
    }
    ta.style.borderColor = '';

    if (sendBtn) setButtonLoading(sendBtn, true);
    try {
        const data = await apiCall(`/api/comments/${commentId}/reply`, 'POST', { reply: text });
        if (data.success) {
            showToast('Reply sent!', 'success');
            toggleReplyForm(commentId);
            // Mark as replied in badges row
            const card = document.getElementById(`comment-${commentId}`);
            if (card) {
                const badgesRow = card.querySelector('.flex.items-center.gap-2.mt-2');
                if (badgesRow && !badgesRow.querySelector('.replied-badge')) {
                    const badge = document.createElement('span');
                    badge.className = 'text-xs text-emerald-500 replied-badge';
                    badge.innerHTML = '<i class="fas fa-check-circle mr-0.5"></i> Replied';
                    badgesRow.appendChild(badge);
                }
            }
        } else {
            showToast(data.error || 'Failed to reply.', 'danger');
            if (sendBtn) setButtonLoading(sendBtn, false);
        }
    } catch (e) {
        showToast('Failed to send reply.', 'danger');
        if (sendBtn) setButtonLoading(sendBtn, false);
    }
}

async function translateComment(commentId, force) {
    const langSelect = document.getElementById(`target-lang-${commentId}`);
    const targetLang = langSelect ? langSelect.value : 'en';
    if (!targetLang) {
        showToast('Please select a target language.', 'warning');
        return;
    }
    const srcSelect = document.getElementById(`source-lang-${commentId}`);
    const sourceLang = srcSelect ? srcSelect.value : 'auto';

    const btn = document.getElementById(`translate-btn-${commentId}`);
    if (btn) setButtonLoading(btn, true);

    try {
        const payload = { target_language: targetLang, source_language: sourceLang };
        if (force) payload.force = true;
        const data = await apiCall(`/api/comments/${commentId}/translate`, 'POST', payload);
        if (data.success) {
            showToast('Translation complete.', 'success');
            const card = document.getElementById(`comment-${commentId}`);
            if (card) {
                const detectedBadge = data.detected_language && data.detected_language !== 'unknown'
                    ? `<span class="text-xs px-1.5 py-0.5 rounded-full bg-indigo-100 text-indigo-700">Detected: ${data.detected_language.charAt(0).toUpperCase() + data.detected_language.slice(1)}</span>`
                    : '';
                const existing = card.querySelector('.translated-text');
                const closeBtn = `<button onclick="this.parentElement.remove()" class="absolute top-1 right-1 text-indigo-400 hover:text-indigo-700 p-0.5 leading-none" title="Dismiss">&times;</button>`;
                const retranslateBtn = `<button onclick="translateComment(${commentId}, true)" class="absolute top-1 right-6 text-indigo-400 hover:text-indigo-700 p-0.5 leading-none" title="Re-translate"><i class="fas fa-redo text-xs"></i></button>`;
                const html = `${closeBtn}${retranslateBtn}<div class="flex items-center gap-2 mb-1"><i class="fas fa-language text-indigo-600"></i>${detectedBadge}</div>${data.translated_text}`;
                if (existing) {
                    existing.innerHTML = html;
                } else {
                    const div = document.createElement('div');
                    div.className = 'translated-text mt-2 p-2 bg-indigo-50 rounded text-sm text-indigo-800 border border-indigo-200 relative';
                    div.innerHTML = html;
                    card.querySelector('.comment-content')?.appendChild(div);
                }
            }
        } else {
            showToast(data.error || 'Translation failed.', 'danger');
        }
    } finally {
        if (btn) setButtonLoading(btn, false);
    }
}

async function flagComment(commentId) {
    const btn = event?.target?.closest('button');
    if (btn) setButtonLoading(btn, true);

    try {
        const data = await apiCall(`/api/comments/${commentId}/flag`, 'POST');
        if (data.success) {
            showToast(data.flagged ? 'Comment flagged.' : 'Comment unflagged.', 'info');
            location.reload();
        }
    } finally {
        if (btn) setButtonLoading(btn, false);
    }
}

async function hideComment(commentId) {
    const btn = event?.target?.closest('button');
    if (btn) setButtonLoading(btn, true);

    try {
        const data = await apiCall(`/api/comments/${commentId}/hide`, 'POST');
        if (data.success) {
            showToast(data.hidden ? 'Comment hidden.' : 'Comment unhidden.', 'info');
            location.reload();
        }
    } finally {
        if (btn) setButtonLoading(btn, false);
    }
}

async function deleteComment(commentId) {
    const ok = await showConfirm({
        title: 'Delete Comment',
        message: 'Delete this comment? This cannot be undone.',
        confirmText: 'Delete',
        type: 'danger',
    });
    if (!ok) return;

    const btn = event?.target?.closest('button');
    if (btn) setButtonLoading(btn, true);

    try {
        const data = await apiCall(`/api/comments/${commentId}/delete`, 'DELETE');
        if (data.success) {
            const card = document.getElementById(`comment-${commentId}`);
            if (card) {
                card.style.transition = 'all 0.3s';
                card.style.opacity = '0';
                card.style.maxHeight = '0';
                card.style.overflow = 'hidden';
                setTimeout(() => card.remove(), 300);
            }
            showToast('Comment deleted.', 'success');
        } else {
            showToast(data.error || 'Failed to delete.', 'danger');
        }
    } finally {
        if (btn) setButtonLoading(btn, false);
    }
}

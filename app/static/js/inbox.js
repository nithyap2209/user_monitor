// Inbox split-panel interactions

async function sendReply(commentId) {
    const textarea = document.getElementById(`reply-textarea-${commentId}`) ||
                     document.getElementById(`reply-input-${commentId}`);
    const reply = textarea?.value?.trim();

    if (!reply) {
        showToast('Please enter a reply.', 'warning');
        return;
    }

    const btn = event?.target?.closest('button');
    if (btn) setButtonLoading(btn, true);

    try {
        const data = await apiCall(`/api/comments/${commentId}/reply`, 'POST', { reply });
        if (data.success) {
            showToast('Reply sent!', 'success');
            textarea.value = '';

            // Update the left panel card to show replied state
            const leftCard = document.getElementById(`inbox-comment-${commentId}`);
            if (leftCard) {
                leftCard.style.transition = 'opacity 0.3s';
                leftCard.style.opacity = '0.5';
                const badge = leftCard.querySelector('.replied-badge');
                if (!badge) {
                    const span = document.createElement('span');
                    span.className = 'replied-badge inline-block w-2 h-2 bg-emerald-400 rounded-full';
                    span.title = 'Replied';
                    leftCard.querySelector('.comment-meta')?.appendChild(span);
                }
            }

            // Show reply in right panel
            const replySection = document.getElementById(`reply-history-${commentId}`);
            if (replySection) {
                replySection.innerHTML = `
                    <div class="bg-emerald-50 border border-emerald-200 rounded-lg p-3 mt-3">
                        <p class="text-xs text-emerald-600 font-medium mb-1">
                            <i class="fas fa-reply mr-1"></i> Your reply (just now)
                        </p>
                        <p class="text-sm text-emerald-800">${reply}</p>
                    </div>
                `;
            }
        } else {
            showToast(data.error || 'Failed to send reply.', 'danger');
        }
    } finally {
        if (btn) setButtonLoading(btn, false);
    }
}

async function translateCommentInbox(commentId) {
    const langSelect = document.getElementById(`target-lang-${commentId}`);
    const targetLang = langSelect ? langSelect.value : 'en';
    if (!targetLang) {
        showToast('Please select a target language.', 'warning');
        return;
    }

    const btn = document.getElementById(`translate-btn-${commentId}`);
    if (btn) setButtonLoading(btn, true);

    try {
        const data = await apiCall(`/api/comments/${commentId}/translate`, 'POST', { target_language: targetLang });
        if (data.success) {
            showToast('Translation complete.', 'success');
            const container = document.getElementById(`translation-${commentId}`);
            if (container) {
                const detectedBadge = data.detected_language && data.detected_language !== 'unknown'
                    ? `<span class="text-xs px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-700">Detected: ${data.detected_language.charAt(0).toUpperCase() + data.detected_language.slice(1)}</span>`
                    : '';
                container.innerHTML = `
                    <div class="bg-indigo-50 border border-indigo-200 rounded-lg p-3 relative">
                        <button onclick="this.parentElement.remove()" class="absolute top-1 right-1 text-indigo-400 hover:text-indigo-700 p-0.5 leading-none text-lg" title="Dismiss">&times;</button>
                        <div class="flex items-center gap-2 mb-1">
                            <i class="fas fa-language text-indigo-600"></i>
                            <span class="text-xs text-indigo-600 font-medium">Translation</span>
                            ${detectedBadge}
                        </div>
                        <p class="text-sm text-indigo-800">${data.translated_text}</p>
                    </div>
                `;
            }
        } else {
            showToast(data.error || 'Translation failed.', 'danger');
        }
    } finally {
        if (btn) setButtonLoading(btn, false);
    }
}

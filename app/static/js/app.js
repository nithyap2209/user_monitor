// ─── Global JS Utilities ────────────────────────────────────

// CSRF token for AJAX
const csrfToken = document.querySelector('meta[name="csrf-token"]')?.content || '';

// ─── API Call Helper ────────────────────────────────────────

async function apiCall(url, method = 'POST', body = null) {
    const opts = {
        method,
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken,
        },
    };
    if (body) opts.body = JSON.stringify(body);

    const res = await fetch(url, opts);
    return res.json();
}

// ─── Toast Notifications ────────────────────────────────────

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const icons = {
        success: 'fa-check-circle',
        danger: 'fa-exclamation-circle',
        warning: 'fa-exclamation-triangle',
        info: 'fa-info-circle',
    };

    const colors = {
        success: 'bg-emerald-50 border-emerald-400 text-emerald-800',
        danger: 'bg-rose-50 border-rose-400 text-rose-800',
        warning: 'bg-amber-50 border-amber-400 text-amber-800',
        info: 'bg-indigo-50 border-indigo-400 text-indigo-800',
    };

    const div = document.createElement('div');
    div.className = `toast-msg flex items-center gap-3 px-4 py-3 rounded-lg shadow-lg text-sm border ${colors[type] || colors.info}`;
    div.innerHTML = `
        <i class="fas ${icons[type] || icons.info}"></i>
        <span class="flex-1">${message}</span>
        <button onclick="this.parentElement.remove()" class="ml-2 hover:opacity-60 text-current">&times;</button>
    `;
    container.appendChild(div);

    setTimeout(() => {
        if (div.parentElement) {
            div.style.transition = 'opacity 0.3s';
            div.style.opacity = '0';
            setTimeout(() => div.remove(), 300);
        }
    }, 5000);
}

// ─── Modal Helpers ──────────────────────────────────────────

function openModal(id) {
    const el = document.getElementById(id);
    if (el) {
        el.classList.remove('hidden');
        document.body.style.overflow = 'hidden';
    }
}

function closeModal(id) {
    const el = document.getElementById(id);
    if (el) {
        el.classList.add('hidden');
        document.body.style.overflow = '';
    }
}

// Close modals on Escape
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        document.querySelectorAll('[id$="Modal"]:not(.hidden), [id$="modal"]:not(.hidden)').forEach((m) => {
            m.classList.add('hidden');
        });
        document.body.style.overflow = '';
    }
});

// ─── Button Loading State ───────────────────────────────────

function setButtonLoading(btn, loading = true) {
    if (loading) {
        btn.dataset.originalText = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = `<svg class="animate-spin h-4 w-4 mr-1 inline" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"/>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
        </svg> Processing...`;
        btn.classList.add('opacity-70', 'cursor-not-allowed');
    } else {
        btn.disabled = false;
        btn.innerHTML = btn.dataset.originalText;
        btn.classList.remove('opacity-70', 'cursor-not-allowed');
    }
}

// ─── Section Loader ─────────────────────────────────────────

function showSectionLoader(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = `
        <div class="w-full py-16 flex items-center justify-center">
            <div class="flex flex-col items-center gap-3">
                <div class="w-8 h-8 border-3 border-indigo-200 border-t-indigo-600 rounded-full animate-spin"></div>
                <p class="text-xs text-gray-400">Loading content...</p>
            </div>
        </div>
    `;
}

function hideSectionLoader(containerId) {
    const loader = document.querySelector(`#${containerId} .section-loader`);
    if (loader) loader.remove();
}

// ─── Auto-dismiss Flash Toasts ──────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.toast-msg').forEach((el) => {
        setTimeout(() => {
            if (el.parentElement) {
                el.style.transition = 'opacity 0.3s';
                el.style.opacity = '0';
                setTimeout(() => el.remove(), 300);
            }
        }, 5000);
    });
});

"""TF-IDF keyword extraction from social media comments."""

import re

from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS


# Extended stop words: sklearn defaults + social-media noise
_EXTRA_STOP = {
    "like", "just", "got", "lol", "lmao", "omg", "btw", "tbh", "imo",
    "yeah", "yes", "hey", "hi", "hello", "thanks", "thank", "please",
    "ok", "okay", "oh", "wow", "really", "thing", "things", "gonna",
    "wanna", "gotta", "don", "doesn", "didn", "isn", "wasn", "aren",
    "won", "wouldn", "couldn", "shouldn", "ve", "ll", "re", "let",
    "know", "think", "want", "make", "good", "great", "nice", "best",
    "video", "comment", "subscribe", "channel", "follow", "post",
}
STOP_WORDS = ENGLISH_STOP_WORDS.union(_EXTRA_STOP)

# Regex patterns for cleaning
_URL_RE = re.compile(r"https?://\S+|www\.\S+")
_MENTION_RE = re.compile(r"@\w+")
_EMOJI_RE = re.compile(
    r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    r"\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U0000FE00-\U0000FE0F"
    r"\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002600-\U000026FF"
    r"\U0000200D\U00002764]+",
    flags=re.UNICODE,
)
_SPECIAL_RE = re.compile(r"[^a-z\s]")


def _clean_text(text):
    """Lowercase, strip URLs, mentions, emojis, and special characters."""
    text = text.lower()
    text = _URL_RE.sub(" ", text)
    text = _MENTION_RE.sub(" ", text)
    text = _EMOJI_RE.sub(" ", text)
    text = _SPECIAL_RE.sub(" ", text)
    return " ".join(text.split())  # collapse whitespace


def extract_top_keywords(comments, top_n=30):
    """Extract the most meaningful keywords from a list of comment strings
    using TF-IDF with unigrams and bigrams.

    Parameters
    ----------
    comments : list[str]
        Raw comment texts.
    top_n : int
        Number of top keywords to return.

    Returns
    -------
    list[dict]
        [{"keyword": "customer service", "score": 0.87}, ...]
        Sorted by descending TF-IDF score, normalised to 0-1.
    """
    if not comments:
        return []

    # Clean every comment
    cleaned = [_clean_text(c) for c in comments if c]
    cleaned = [c for c in cleaned if len(c) > 2]  # drop empty/tiny

    if len(cleaned) < 2:
        return []

    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2),
        stop_words=list(STOP_WORDS),
        min_df=2,
        max_df=0.85,
        max_features=5000,
        sublinear_tf=True,
    )

    try:
        tfidf_matrix = vectorizer.fit_transform(cleaned)
    except ValueError:
        # All terms filtered out (e.g. too few unique tokens)
        return []

    feature_names = vectorizer.get_feature_names_out()

    # Sum TF-IDF scores across all documents for each term
    scores = tfidf_matrix.sum(axis=0).A1  # dense 1-D array

    # Rank descending
    ranked_indices = scores.argsort()[::-1]

    # Normalise scores to 0-1 range
    max_score = scores[ranked_indices[0]] if len(ranked_indices) else 1.0
    if max_score == 0:
        max_score = 1.0

    results = []
    for idx in ranked_indices[:top_n]:
        keyword = feature_names[idx]
        normalised = round(float(scores[idx] / max_score), 4)
        if normalised > 0:
            results.append({"keyword": keyword, "score": normalised})

    return results


# Backwards-compatible helper for per-comment keyword extraction
def extract_keywords(text, max_keywords=10):
    """Extract keywords from a single comment (simple frequency fallback)."""
    if not text:
        return []

    cleaned = _clean_text(text)
    words = cleaned.split()
    filtered = [w for w in words if w not in STOP_WORDS and len(w) > 2]

    freq = {}
    for w in filtered:
        freq[w] = freq.get(w, 0) + 1

    sorted_words = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return [w for w, _ in sorted_words[:max_keywords]]
"""Centralized Anthropic Claude API service for AI features.

All AI-powered features (sentiment analysis, translation, keyword extraction)
go through this module so there's a single place to manage the API key,
model selection, and error handling.
"""

import os
import json
import anthropic


def _get_client(api_key=None):
    """Return an Anthropic client, using the provided key or env var."""
    key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not key:
        return None
    return anthropic.Anthropic(api_key=key)


def analyze_sentiment(text, api_key=None):
    """Classify comment sentiment using Claude.

    Returns:
        dict with 'sentiment' (str) and 'score' (float 0-1).
        Sentiment is one of: positive, negative, neutral, lead, business.
    """
    if not text or not text.strip():
        return {"sentiment": "neutral", "score": 0.5}

    client = _get_client(api_key)
    if not client:
        return _heuristic_sentiment(text)

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Classify the sentiment of this social media comment. "
                        "Respond with ONLY a JSON object with two keys:\n"
                        '- "sentiment": one of "positive", "negative", "neutral", "lead", "business"\n'
                        '- "score": confidence float between 0.0 and 1.0\n\n'
                        'Use "lead" if the comment contains contact info (phone, email) or '
                        'asks to be contacted. Use "business" if it\'s a business inquiry '
                        "or partnership request.\n\n"
                        f"Comment: {text}"
                    ),
                }
            ],
        )
        result = json.loads(message.content[0].text.strip())
        sentiment = result.get("sentiment", "neutral")
        score = float(result.get("score", 0.5))
        if sentiment not in ("positive", "negative", "neutral", "lead", "business"):
            sentiment = "neutral"
        return {"sentiment": sentiment, "score": max(0.0, min(1.0, score))}
    except Exception:
        return _heuristic_sentiment(text)


def get_supported_languages():
    """Return supported languages as a dict of {name: code}.

    Uses Google Translate via deep-translator.
    """
    from deep_translator import GoogleTranslator
    return GoogleTranslator().get_supported_languages(as_dict=True)


def translate_text(text, target_language="en", source_language="auto"):
    """Translate text to the target language.

    Strategy:
    1. Detect if text is Tanglish/Hinglish using word patterns.
    2. If Tanglish/Hinglish detected → use Claude API (most accurate).
    3. Otherwise → use Google Translate.
    4. If Google Translate fails (returns similar text) → try Claude.
    5. Final fallback → transliteration + Google Translate pipeline.

    Args:
        text: The text to translate.
        target_language: Language code (e.g. 'en', 'es', 'fr').
        source_language: Language code or "auto".  When set explicitly,
            transliteration is applied before translation.

    Returns:
        dict with 'translated_text' (str) and 'detected_language' (str).
    """
    if not text or not text.strip():
        return {"translated_text": text, "detected_language": "unknown"}

    from deep_translator import GoogleTranslator

    try:
        source = source_language if source_language != "auto" else "auto"

        # Step 1: Detect Tanglish/Hinglish from word patterns
        detected_indian = None
        if _is_latin_script(text) and not _looks_like_english(text):
            detected_indian = _detect_indian_language(text)

        # Step 1b: Detect mixed-script text (e.g. English + Tamil/Hindi script)
        # Google Translate often mistranslates these — Claude handles them better
        if _is_mixed_script(text):
            claude_result = _translate_with_claude(text, target_language)
            if claude_result:
                return claude_result

        # Step 2: If Tanglish/Hinglish detected, try Claude first (most accurate)
        if detected_indian:
            claude_result = _translate_with_claude(text, target_language, detected_indian)
            if claude_result:
                return claude_result

            # Claude failed — try transliteration with detected language
            # DON'T fall through to Google auto-detect (it misidentifies Tamil as Malayalam etc.)
            translit_result = _try_transliterate_and_translate(text, target_language, detected_indian)
            if translit_result:
                return {
                    "translated_text": translit_result["translated"],
                    "detected_language": _lang_code_to_name(translit_result["lang_code"]),
                }

        # Step 3: Try Google Translate
        text_to_translate = text

        # When source language is explicitly set and text is in Latin script,
        # transliterate to native script first
        if source != "auto" and _is_latin_script(text):
            native_text = _transliterate_to_native(text, source)
            if native_text and native_text != text:
                text_to_translate = native_text

        translator = GoogleTranslator(source=source, target=target_language)
        translated = translator.translate(text_to_translate)

        # Step 4: If Google didn't translate properly, try Claude then transliteration
        if _texts_are_similar(text, translated) and not _looks_like_english(text):
            # Try Claude even without detected language hint
            claude_result = _translate_with_claude(text, target_language)
            if claude_result:
                return claude_result

            # Final fallback: transliteration across Indian languages
            best = _try_transliterate_and_translate(text, target_language, source)
            if best:
                translated = best["translated"]
                detected_code = best["lang_code"]
                detected_lang = _lang_code_to_name(detected_code)
                return {
                    "translated_text": translated,
                    "detected_language": detected_lang,
                }

        detected_lang = _detect_language_name(text)
        return {
            "translated_text": translated,
            "detected_language": detected_lang,
        }
    except Exception:
        return {"translated_text": f"[Translation failed] {text}", "detected_language": "unknown"}


_COMMON_ENGLISH = {
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "may", "might", "must", "can", "could", "i", "you", "he",
    "she", "it", "we", "they", "me", "him", "her", "us", "them", "my",
    "your", "his", "its", "our", "their", "this", "that", "these", "those",
    "what", "which", "who", "whom", "where", "when", "why", "how",
    "not", "no", "yes", "and", "or", "but", "if", "so", "for", "of",
    "in", "on", "at", "to", "from", "by", "with", "about", "as", "up",
    "out", "all", "very", "just", "also", "than", "more", "most", "like",
    "good", "great", "nice", "best", "bad", "new", "old", "big", "small",
    "get", "got", "go", "going", "come", "came", "make", "made", "take",
    "know", "think", "want", "need", "see", "look", "give", "use",
    "find", "tell", "say", "said", "work", "call", "try", "ask",
    "feel", "leave", "put", "mean", "keep", "let", "begin", "show",
    "here", "there", "now", "then", "well", "too", "any", "each",
    "only", "some", "such", "other", "into", "over", "after", "before",
    "much", "many", "same", "long", "way", "because", "still", "through",
    "been", "being", "really", "please", "thank", "thanks", "hello", "hi",
    "okay", "ok", "sure", "right", "time", "day", "today", "tomorrow",
    # Common English words used in Tanglish/Hinglish
    "price", "cost", "costly", "product", "service", "delivery", "quality",
    "order", "phone", "mobile", "number", "email", "website", "app",
    "super", "level", "class", "style", "model", "brand", "color",
    "colour", "size", "speed", "power", "range", "battery", "charge",
    "review", "rating", "offer", "discount", "sale", "deal", "free",
    "buy", "sell", "shop", "store", "online", "offline", "fast", "slow",
    "problem", "issue", "fix", "update", "version", "install", "download",
    "video", "photo", "image", "camera", "screen", "display", "sound",
    "music", "song", "movie", "game", "team", "match", "player", "score",
    "bro", "dude", "sir", "madam", "boss", "friend", "guys",
    "awesome", "amazing", "cool", "worst", "fake", "real", "original",
    "first", "last", "next", "full", "half", "double", "single", "extra",
    "happy", "sad", "love", "hate", "wait", "help", "send", "share",
}


# Common Tamil words written in English script (Tanglish indicators)
_TANGLISH_WORDS = {
    "nalla", "irukku", "illa", "illai", "enna", "epdi", "eppadi", "paru",
    "paaru", "paarunga", "parunka", "poi", "vaa", "vaanga", "inga", "anga",
    "enga", "sollu", "sollunka", "sollunga", "panna", "pannunka", "pannunga",
    "panren", "pannuven", "pannalam", "venum", "vendum", "mudiyum", "mudiyala",
    "theriyum", "theriyala", "theriyadhu", "puriyadhu", "puriyala", "romba",
    "konjam", "thaan", "dhaan", "amma", "anna", "akka", "thambi",
    "vanakkam", "nandri", "enakku", "unnoda", "ungalukku", "neenga", "neengal",
    "naan", "naanga", "avanga", "ivanga", "ithu", "athu", "oru",
    "irukken", "irukkanga", "iruku", "aana", "aanalum", "aanaa",
    "innum", "innaiku", "nalaiku", "naalaikku", "naalaiku", "mela", "keela", "pakka",
    "varadhu", "varuvaa", "varuven", "varala", "poradhu", "kudukka", "edukka",
    "vara", "varaa", "varaadhu", "varaathu", "varen", "vareenga", "varuvom",
    "sonna", "senju", "vantha", "pona", "vandhu", "nenachchu",
    "padikka", "kelunka", "kelungka", "sari", "da", "di", "la", "le",
    "ku", "oda", "kku", "nu", "nga", "dha", "dhu", "thu",
    "kalyanam", "velai", "padam", "pattu", "kadai", "veetla", "oorla",
    "mattum", "thalli", "thamizh", "semma", "vera", "mass", "gethu", "sema",
    "machi", "machan", "machaan", "nanba", "thala", "dei", "mairu",
    "evlo", "yevlo", "ethanai", "yaar", "yaaru", "edhuku", "ethuku",
    "podu", "podunka", "edhu", "endha", "inga", "pakkam",
    "sapadu", "thanni", "ooru", "veedu", "paiyyan", "ponnu",
    "kaasu", "vanga", "kudu", "thaa", "vaada", "poda", "pessu",
    "start", "pannunka", "stop", "pannunga", "course",
    "mala", "malai", "maalai", "mazhai", "kaalaila", "madhiyam", "raathiri",
    "pola", "maathiri", "thala", "ennoda", "unnoda", "avlo", "ivlo",
    "poganum", "varanam", "pannanum", "sollanam", "paakanum",
    "ennachu", "ennaachu", "aachu", "pochu", "vandhaachu",
    "kaduppu", "mokka", "paavam", "azhaga", "azhagaa",
    "pogalaam", "vaalaam", "panlaam", "solra", "solranga", "panra", "panranga",
    "irundha", "irundhaal", "vandha", "pona", "sonna",
    "therinja", "purinja", "kedaichu", "kedaikum", "kedaikala",
}

# Common Hindi words written in English script (Hinglish indicators)
_HINGLISH_WORDS = {
    "kya", "hai", "nahi", "nahin", "accha", "achha", "bahut", "bohot",
    "kaisa", "kaise", "aur", "mein", "tum", "hum", "aap", "kuch",
    "haan", "theek", "thik", "karke", "karo", "karna", "bhai", "yaar",
    "lekin", "magar", "isliye", "kyunki", "kahan", "kidhar", "idhar",
    "udhar", "abhi", "baad", "pehle", "jaise", "waise", "matlab",
    "samajh", "samajhna", "dekho", "dekhna", "suno", "sunna",
    "bolo", "bolna", "chalo", "chaliye", "aana", "jaana",
    "khana", "peena", "paisa", "paise", "ghar", "dost", "pyaar",
    "agar", "toh", "bhi", "zaroor", "zarur", "sach", "jhooth",
    "padhai", "kaam", "wala", "wali", "waala", "waali",
    "bilkul", "ekdum", "sachme", "sacchi", "pakka",
    "didi", "bhaiya", "chacha", "beta", "beti", "baccha",
    "achha", "theek", "sahi", "galat", "mushkil", "aasaan",
    "zyada", "thoda", "bahut", "kitna", "kaun", "kisko",
    "suniye", "batao", "bataye", "dijiye", "lijiye",
    "raha", "rahi", "raho", "chala", "gaya", "gayi",
    "milega", "milegi", "chahiye", "sakta", "sakti",
}

# Tamil-specific suffixes that indicate Tanglish
_TAMIL_SUFFIXES = (
    "nka", "nga", "kku", "oda", "dhu", "thu", "chu", "nnu",
    "lla", "kka", "ven", "vaa", "lam", "num", "yum", "ala",
    "adhu", "idhu", "unka", "unga", "inga", "raa", "aadhu",
    "aathu", "anum", "anga", "onga", "laam", "ren", "rom",
)

# Hindi-specific suffixes that indicate Hinglish
_HINDI_SUFFIXES = (
    "iye", "oge", "ogi", "ega", "egi", "tha", "thi",
    "kar", "raha", "rahi", "wala", "wali", "enge",
)


def _detect_indian_language(text):
    """Detect if Latin-script text is Tanglish, Hinglish, or another Indian language.

    Returns language code ('ta' for Tamil, 'hi' for Hindi) or None.
    """
    words = text.strip().lower().split()
    if not words:
        return None

    clean_words = [w.rstrip(".,!?;:'\"") for w in words]

    # Count matches against known word sets (excluding common English words)
    tamil_score = 0
    hindi_score = 0

    for w in clean_words:
        if w in _COMMON_ENGLISH:
            continue
        if w in _TANGLISH_WORDS:
            tamil_score += 1
        if w in _HINGLISH_WORDS:
            hindi_score += 1
        # Check suffixes for words not in dictionaries
        if len(w) > 3:
            if any(w.endswith(s) for s in _TAMIL_SUFFIXES):
                tamil_score += 0.5
            if any(w.endswith(s) for s in _HINDI_SUFFIXES):
                hindi_score += 0.5

    if tamil_score > hindi_score and tamil_score >= 1:
        return "ta"
    if hindi_score > tamil_score and hindi_score >= 1:
        return "hi"
    if tamil_score > 0:
        return "ta"
    if hindi_score > 0:
        return "hi"

    return None


def _translate_with_claude(text, target_language, detected_lang_hint=None):
    """Use Claude to translate mixed-language text (Tanglish/Hinglish etc.).

    Claude natively understands transliterated Indian languages, making it
    far more accurate than Google Translate for Tanglish/Hinglish.
    """
    client = _get_client()
    if not client:
        return None

    lang_names = {
        "ta": "Tamil", "hi": "Hindi", "te": "Telugu", "kn": "Kannada",
        "ml": "Malayalam", "bn": "Bengali", "mr": "Marathi", "gu": "Gujarati",
        "pa": "Punjabi", "ur": "Urdu",
    }
    target_name = _lang_code_to_name(target_language) or target_language

    hint = ""
    if detected_lang_hint:
        hint_name = lang_names.get(detected_lang_hint, detected_lang_hint)
        hint = (
            f"\n\nIMPORTANT: This text is {hint_name} written in English/Latin "
            f"letters (transliterated). For example, Tanglish is Tamil words "
            f"written in English script. Translate based on the {hint_name} meaning "
            f"of the words, NOT their English appearance."
        )

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Translate the following social media comment to {target_name}.\n\n"
                        "The text is written in an Indian language using English/Latin "
                        "letters (transliterated). Common examples:\n"
                        "- Tanglish: Tamil words in English script (e.g. 'nalaiku' = tomorrow, "
                        "'anna' = brother/bro, 'varaa' = come, 'mala/mazhai' = rain, "
                        "'illa' = no/not, 'dhu' = negation suffix)\n"
                        "- Hinglish: Hindi words in English script (e.g. 'kal' = tomorrow, "
                        "'bhai' = brother, 'nahi' = no)\n"
                        f"{hint}\n\n"
                        "Respond with ONLY a JSON object with two keys:\n"
                        '- "translated_text": the accurate English translation\n'
                        '- "detected_language": the original language name in lowercase '
                        '(e.g. "tamil", "hindi", "telugu")\n\n'
                        f"Text: {text}"
                    ),
                }
            ],
        )
        raw = message.content[0].text.strip()
        # Handle case where Claude wraps JSON in markdown code blocks
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(raw)
        translated = result.get("translated_text", "")
        detected = result.get("detected_language", "unknown")
        if translated and not _texts_are_similar(text, translated):
            return {
                "translated_text": translated,
                "detected_language": detected,
            }
    except Exception:
        pass
    return None


def _looks_like_english(text):
    """Check if text appears to be actual English (not transliterated)."""
    words = text.strip().lower().split()
    if not words:
        return False
    english_count = sum(1 for w in words if w.rstrip(".,!?;:'\"") in _COMMON_ENGLISH)
    return (english_count / len(words)) > 0.5


def _texts_are_similar(original, translated):
    """Check if translated text is too similar to original (translation didn't work)."""
    if not translated:
        return True
    orig = original.strip().lower()
    trans = translated.strip().lower()
    if orig == trans:
        return True
    # Check character-level similarity for near-matches
    if len(orig) == 0:
        return True
    common = sum(1 for a, b in zip(orig, trans) if a == b)
    similarity = common / max(len(orig), len(trans))
    return similarity > 0.85


# Common Indian languages that are frequently typed in Latin script
_TRANSLITERATION_LANGS = ["ta", "hi", "te", "kn", "ml", "bn", "mr", "gu", "pa", "ur"]


def _try_transliterate_and_translate(text, target_language, source_hint="auto"):
    """Try transliterating text to multiple Indian languages and translate.

    Returns the best result (most different from original) or None.
    """
    from deep_translator import GoogleTranslator

    # Build priority list: detected language first, then source hint, then defaults
    langs_to_try = list(_TRANSLITERATION_LANGS)

    detected = _detect_indian_language(text)
    if detected and detected in langs_to_try:
        langs_to_try.remove(detected)
        langs_to_try.insert(0, detected)

    if source_hint != "auto" and source_hint in langs_to_try:
        langs_to_try.remove(source_hint)
        langs_to_try.insert(0, source_hint)

    best_result = None
    best_score = 0

    for lang_code in langs_to_try[:5]:  # Try top 5 languages
        try:
            native_text = _transliterate_to_native(text, lang_code)
            if not native_text or native_text == text:
                continue

            # Check how much of the text was actually transliterated
            # (non-Latin chars indicate the transliteration worked)
            native_non_latin = sum(1 for c in native_text if c.isalpha() and not c.isascii())
            native_total = sum(1 for c in native_text if c.isalpha())
            translit_ratio = native_non_latin / max(native_total, 1)

            translated = GoogleTranslator(
                source=lang_code, target=target_language
            ).translate(native_text)

            if not translated or _texts_are_similar(text, translated):
                continue

            # Score: how different the translation is + how well transliteration worked
            orig_lower = text.strip().lower()
            trans_lower = translated.strip().lower()
            common = sum(1 for a, b in zip(orig_lower, trans_lower) if a == b)
            diff = 1 - (common / max(len(orig_lower), len(trans_lower), 1))

            # Combined score: translation difference + transliteration quality
            score = (diff * 0.6) + (translit_ratio * 0.4)

            if score > best_score:
                best_score = score
                best_result = {"translated": translated, "lang_code": lang_code}
        except Exception:
            continue

    return best_result


def _is_latin_script(text):
    """Check if text is primarily in Latin (ASCII) script."""
    latin_count = sum(1 for c in text if c.isascii() and c.isalpha())
    total_alpha = sum(1 for c in text if c.isalpha())
    if total_alpha == 0:
        return False
    return (latin_count / total_alpha) > 0.7


def _is_mixed_script(text):
    """Check if text contains both Latin and non-Latin alphabetic characters.

    Mixed-script text (e.g. English + Tamil/Hindi script) confuses Google
    Translate which often mistranslates the English words.
    """
    has_latin = False
    has_non_latin = False
    for c in text:
        if c.isalpha():
            if c.isascii():
                has_latin = True
            else:
                has_non_latin = True
        if has_latin and has_non_latin:
            return True
    return False


def _transliterate_to_native(text, lang_code):
    """Convert Latin-script text to native script using Google Input Tools.

    Processes text word-by-word, skipping English words to handle
    mixed Tanglish/Hinglish text correctly.
    """
    import requests as _req
    try:
        words = text.split()
        result_words = []
        for word in words:
            clean = word.rstrip(".,!?;:'\"").lower()
            # Keep English words as-is (don't transliterate them)
            if clean in _COMMON_ENGLISH or (len(clean) <= 2 and clean.isascii()):
                result_words.append(word)
                continue
            resp = _req.get(
                "https://inputtools.google.com/request",
                params={
                    "text": word,
                    "itc": f"{lang_code}-t-i0-und",
                    "num": 1,
                    "cp": 0,
                    "cs": 1,
                    "ie": "utf-8",
                    "oe": "utf-8",
                },
                timeout=5,
            )
            data = resp.json()
            if data[0] == "SUCCESS" and data[1]:
                candidates = data[1][0][1]
                result_words.append(candidates[0] if candidates else word)
            else:
                result_words.append(word)
        return " ".join(result_words)
    except Exception:
        pass
    return text


def _detect_language_code(text):
    """Detect language code (e.g. 'ta', 'hi') via Google Translate auto-detect."""
    import requests as _req
    try:
        resp = _req.get(
            "https://translate.googleapis.com/translate_a/single",
            params={
                "client": "gtx",
                "sl": "auto",
                "tl": "en",
                "dt": "t",
                "q": text[:200],
            },
            timeout=5,
        )
        data = resp.json()
        return data[2] if len(data) > 2 else "unknown"
    except Exception:
        return "unknown"


def _detect_language_name(text):
    """Detect the language name (e.g. 'tamil', 'hindi') of the text."""
    try:
        code = _detect_language_code(text)
        return _lang_code_to_name(code)
    except Exception:
        return "unknown"


def _lang_code_to_name(code):
    """Convert a language code to its name."""
    try:
        from deep_translator.constants import GOOGLE_LANGUAGES_TO_CODES
        code_to_name = {v: k for k, v in GOOGLE_LANGUAGES_TO_CODES.items()}
        return code_to_name.get(code, code)
    except Exception:
        return code


def extract_keywords(text, max_keywords=10, api_key=None):
    """Extract keywords/topics from text using Claude.

    Returns:
        list of keyword strings.
    """
    if not text or not text.strip():
        return []

    client = _get_client(api_key)
    if not client:
        return []

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Extract up to {max_keywords} relevant keywords or topics "
                        "from this social media comment. "
                        "Respond with ONLY a JSON array of strings.\n\n"
                        f"Comment: {text}"
                    ),
                }
            ],
        )
        result = json.loads(message.content[0].text.strip())
        if isinstance(result, list):
            return [str(k) for k in result[:max_keywords]]
        return []
    except Exception:
        return []


_POSITIVE_EMOJIS = set("😊😍❤👍🙏😁🥰💯🔥✨💪👏😀🎉😃😄🤩😇💖💕♥👌🤗😎💗🌟⭐🏆")
_NEGATIVE_EMOJIS = set("😡😠👎💔😤😢😭🤮🤬😒😞😔😩😫🤢😑😶🙄😕😖😣💀☠👊")


def _heuristic_sentiment(text, skip_translate=False):
    """Keyword-based sentiment fallback when no API key is available.

    Translates non-English text to English first using Google Translate
    so that keyword matching works across all languages.
    Set skip_translate=True for bulk processing to avoid slow API calls.
    """
    text_lower = text.lower()

    # Translate non-English text to English for keyword matching
    if not skip_translate and not _is_latin_script(text):
        try:
            from deep_translator import GoogleTranslator
            translated = GoogleTranslator(source="auto", target="en").translate(text)
            if translated:
                text_lower = translated.lower()
        except Exception:
            pass  # fall through with original text

    positive_words = {
        "good", "great", "love", "excellent", "amazing", "best",
        "awesome", "thank", "happy", "wonderful", "fantastic", "perfect",
        "beautiful", "brilliant", "impressed", "recommend", "satisfied",
        "helpful", "nice", "superb", "outstanding", "incredible",
        "appreciate", "favorite", "favourite", "worth", "reliable",
    }
    negative_words = {
        "bad", "terrible", "worst", "hate", "awful", "poor",
        "horrible", "scam", "fraud", "disgusting", "disappointed", "useless",
        "waste", "regret", "cheat", "fake", "broken", "pathetic",
        "rubbish", "ridiculous", "angry", "complaint", "sucks",
        "overpriced", "defective", "not worth", "no solution",
        "don't buy", "do not buy", "never buy", "rip off", "ripoff",
        "not recommend", "unreliable", "frustrat", "mislead", "liar",
        "damage", "refund", "problem", "issue", "fail", "worse",
    }
    lead_words = {"call me", "contact me", "my number", "my email", "reach me", "phone"}

    for phrase in lead_words:
        if phrase in text_lower:
            return {"sentiment": "lead", "score": 0.8}

    pos = sum(1 for w in positive_words if w in text_lower)
    neg = sum(1 for w in negative_words if w in text_lower)

    # Emoji-based sentiment (works for all languages without translation)
    for ch in text:
        if ch in _POSITIVE_EMOJIS:
            pos += 1
        elif ch in _NEGATIVE_EMOJIS:
            neg += 1

    if pos > neg:
        return {"sentiment": "positive", "score": min(0.5 + pos * 0.1, 1.0)}
    elif neg > pos:
        return {"sentiment": "negative", "score": max(0.5 - neg * 0.1, 0.0)}
    return {"sentiment": "neutral", "score": 0.5}

"""Centralized Anthropic Claude API service for AI features.

All AI-powered features (sentiment analysis, translation, keyword extraction)
go through this module so there's a single place to manage the API key,
model selection, and error handling.
"""

import os
import json
import logging
import anthropic

logger = logging.getLogger(__name__)

# ── VADER NLP Sentiment Analyzer (lazy-loaded singleton) ──
_vader_analyzer = None


def _get_vader():
    """Return a VADER SentimentIntensityAnalyzer, downloading data if needed."""
    global _vader_analyzer
    if _vader_analyzer is not None:
        return _vader_analyzer
    try:
        import nltk
        try:
            from nltk.sentiment.vader import SentimentIntensityAnalyzer
            _vader_analyzer = SentimentIntensityAnalyzer()
        except LookupError:
            nltk.download("vader_lexicon", quiet=True)
            from nltk.sentiment.vader import SentimentIntensityAnalyzer
            _vader_analyzer = SentimentIntensityAnalyzer()
        return _vader_analyzer
    except Exception:
        return None


def _textblob_sentiment(text):
    """Return TextBlob polarity and subjectivity for text.

    Returns:
        tuple (polarity: float [-1,1], subjectivity: float [0,1])
        or (0.0, 0.0) on failure.
    """
    try:
        from textblob import TextBlob
        blob = TextBlob(text)
        return (blob.sentiment.polarity, blob.sentiment.subjectivity)
    except Exception:
        return (0.0, 0.0)


def _nlp_preprocess(text):
    """NLP preprocessing: tokenize, lemmatize, remove stop words.

    Used for feature extraction (TextBlob, TF-IDF). Does NOT modify the
    original text used for VADER — VADER needs capitalization, punctuation,
    and emojis intact.
    """
    try:
        import nltk
        from nltk.tokenize import word_tokenize
        from nltk.stem import WordNetLemmatizer
        from nltk.corpus import stopwords

        try:
            stop_words = set(stopwords.words("english"))
        except LookupError:
            nltk.download("stopwords", quiet=True)
            nltk.download("punkt_tab", quiet=True)
            nltk.download("wordnet", quiet=True)
            stop_words = set(stopwords.words("english"))

        # Keep negation words — critical for sentiment analysis
        negation_words = {"not", "no", "never", "nor", "neither", "hardly", "barely"}
        stop_words -= negation_words

        lemmatizer = WordNetLemmatizer()
        tokens = word_tokenize(text.lower())
        cleaned = [
            lemmatizer.lemmatize(token)
            for token in tokens
            if token.isalpha() and token not in stop_words
        ]
        return " ".join(cleaned)
    except Exception:
        return text.lower()


def _get_client(api_key=None):
    """Return an Anthropic client, using the provided key or env var."""
    key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not key:
        return None
    return anthropic.Anthropic(api_key=key)


# ── OpenAI GPT client (lazy-loaded singleton) ──
_openai_client = None


def _get_openai_client():
    """Return an OpenAI client if OPENAI_API_KEY is set."""
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return None
    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=key)
        return _openai_client
    except Exception:
        return None


def _get_openai_model():
    """Return the configured OpenAI model (default: gpt-4o-mini)."""
    return os.getenv("OPENAI_MODEL", "gpt-4o-mini")


_GPT_SENTIMENT_PROMPT = (
    "You are a sentiment analysis expert specializing in multilingual social media comments. "
    "You understand Tanglish (Tamil written in English script), Hinglish (Hindi written in English script), "
    "Tamil script, Hindi script, and all major Indian languages mixed with English.\n\n"
    "TANGLISH EXAMPLES:\n"
    "- 'semma video da' → positive (means 'great video man')\n"
    "- 'mokka content' → negative (means 'boring/bad content')\n"
    "- 'nalla irukku' → positive (means 'it is good')\n"
    "- 'Bro anga placements la iruka?' → neutral (asking about placements)\n"
    "- 'சூப்பர் சார்' → positive (means 'super sir')\n"
    "- 'vera level thalaiva' → positive (means 'next level boss')\n"
    "- 'kaduppu content' → negative (means 'annoying content')\n\n"
    "TAMIL SCRIPT EXAMPLES:\n"
    "- 'நல்ல வீடியோ' → positive (means 'good video')\n"
    "- 'மிகவும் மோசம்' → negative (means 'very bad')\n\n"
    "RULES:\n"
    "1. Classify as: positive, negative, neutral, lead, or business\n"
    "2. Use 'lead' if the comment contains contact info (phone, email) or asks to be contacted\n"
    "3. Use 'business' for business inquiries or partnership requests\n"
    "4. Understand slang, abbreviations, emojis, sarcasm\n"
    "5. Score is confidence from 0.0 to 1.0\n\n"
    "Respond with ONLY a JSON object: {\"sentiment\": \"...\", \"score\": 0.0}\n\n"
    "Comment: "
)


def _analyze_with_gpt(text):
    """Analyze sentiment using OpenAI GPT (GPT-4o, GPT-4, GPT-3.5).

    Particularly strong at understanding Tanglish, Hinglish, Tamil script,
    sarcasm, and nuanced social media language.

    Returns:
        dict with 'sentiment' and 'score', or None on failure.
    """
    client = _get_openai_client()
    if not client:
        return None

    try:
        response = client.chat.completions.create(
            model=_get_openai_model(),
            max_tokens=80,
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": "You are a sentiment classifier. Respond with only JSON.",
                },
                {
                    "role": "user",
                    "content": _GPT_SENTIMENT_PROMPT + text,
                },
            ],
        )
        raw = response.choices[0].message.content.strip()
        # Handle markdown code blocks
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(raw)
        sentiment = result.get("sentiment", "neutral")
        score = float(result.get("score", 0.5))
        if sentiment not in ("positive", "negative", "neutral", "lead", "business"):
            sentiment = "neutral"
        logger.info("GPT sentiment for '%s': %s (%.2f)", text[:60], sentiment, score)
        return {"sentiment": sentiment, "score": max(0.0, min(1.0, score))}
    except Exception as e:
        logger.warning("GPT sentiment failed for '%s': %s", text[:60], e)
        return None


def analyze_sentiment(text, api_key=None):
    """Classify comment sentiment using GPT → Claude → VADER+TextBlob ensemble.

    Priority:
    1. OpenAI GPT (if OPENAI_API_KEY set) — best for Tanglish/multilingual
    2. Anthropic Claude (if ANTHROPIC_API_KEY set)
    3. VADER + TextBlob + regional keywords ensemble (always available)

    Returns:
        dict with 'sentiment' (str) and 'score' (float 0-1).
        Sentiment is one of: positive, negative, neutral, lead, business.
    """
    if not text or not text.strip():
        return {"sentiment": "neutral", "score": 0.5}

    # ── Try GPT first (best for Tanglish/multilingual) ──
    gpt_result = _analyze_with_gpt(text)
    if gpt_result:
        return gpt_result

    # ── Try Claude ──
    client = _get_client(api_key)
    if client:
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
            pass

    # ── Fallback: VADER + TextBlob + regional keywords ensemble ──
    return _heuristic_sentiment(text)


def get_supported_languages():
    """Return supported languages as a dict of {name: code}.

    Uses Google Translate via deep-translator.
    """
    from deep_translator import GoogleTranslator
    return GoogleTranslator().get_supported_languages(as_dict=True)


def translate_text(text, target_language="en", source_language="auto"):
    """Translate text using deep-translator + py-trans (dual engine).

    Strategy:
    1. Hinglish → deep-translator with source='hi' (works natively).
    2. First attempt: deep-translator (GoogleTranslator) with auto-detect.
    3. If echoed back: py-trans Google engine (different auto-detection).
    4. If still echoed: py-trans MyMemory engine.
    5. Fallback: transliterate known Tamil/Hindi words → retry translation.
    6. All other languages → deep-translator with source='auto'.

    Args:
        text: The text to translate.
        target_language: Language code (e.g. 'en', 'es', 'fr').
        source_language: Language code or "auto".

    Returns:
        dict with 'translated_text' (str) and 'detected_language' (str).
    """
    if not text or not text.strip():
        return {"translated_text": text, "detected_language": "unknown"}

    from deep_translator import GoogleTranslator

    try:
        source = source_language if source_language != "auto" else "auto"

        # Detect Tanglish/Hinglish in Latin-script text
        detected_indian = None
        if _is_latin_script(text):
            detected_indian = _detect_indian_language(text)

        # ── Hinglish: Google Translate handles Latin-script Hindi natively ──
        if detected_indian == "hi":
            translated = GoogleTranslator(source="hi", target=target_language).translate(text)
            if translated and not _texts_are_similar(text, translated):
                return {"translated_text": translated, "detected_language": "hinglish"}

        # ── Tanglish pipeline: aggressive transliterate → Tamil → English ──
        if detected_indian == "ta":
            tamil_text = _transliterate_aggressive(text, "ta")
            if tamil_text and tamil_text != text:
                # Check that meaningful transliteration occurred
                native_chars = sum(1 for c in tamil_text if c.isalpha() and not c.isascii())
                total_chars = sum(1 for c in tamil_text if c.isalpha())
                if total_chars > 0 and (native_chars / total_chars) > 0.2:
                    try:
                        translated = GoogleTranslator(
                            source="ta", target=target_language
                        ).translate(tamil_text)
                        if translated and not _texts_are_similar(text, translated):
                            return {
                                "translated_text": translated,
                                "detected_language": "tanglish",
                            }
                    except Exception:
                        pass

        # ── First attempt: deep-translator with auto-detect ──
        text_to_translate = text

        # When source language is explicitly set and text is Latin script,
        # transliterate known regional words to native script first
        if source != "auto" and _is_latin_script(text):
            native_text = _transliterate_to_native(text, source)
            if native_text and native_text != text:
                text_to_translate = native_text

        translated = GoogleTranslator(source=source, target=target_language).translate(text_to_translate)

        # If translation worked, return it
        if translated and not _texts_are_similar(text, translated):
            detected_lang = _detect_language_name(text)
            if detected_indian == "ta":
                detected_lang = "tanglish"
            elif detected_indian == "hi":
                detected_lang = "hinglish"
            elif detected_indian == "mixed":
                detected_lang = "mixed"
            return {"translated_text": translated, "detected_language": detected_lang}

        # ── Second attempt: py-trans Google engine (different auto-detection) ──
        pytrans_result = _translate_with_pytrans(text, target_language, engine="google")
        if pytrans_result:
            detected_lang = pytrans_result.get("origin_lang", "unknown")
            if detected_indian == "ta":
                detected_lang = "tanglish"
            elif detected_indian == "hi":
                detected_lang = "hinglish"
            return {"translated_text": pytrans_result["translation"], "detected_language": detected_lang}

        # ── Third attempt: py-trans MyMemory engine ──
        pytrans_mm = _translate_with_pytrans(text, target_language, engine="my_memory")
        if pytrans_mm:
            detected_lang = pytrans_mm.get("origin_lang", "unknown")
            if detected_indian == "ta":
                detected_lang = "tanglish"
            elif detected_indian == "hi":
                detected_lang = "hinglish"
            return {"translated_text": pytrans_mm["translation"], "detected_language": detected_lang}

        # ── Transliteration pipeline fallback ──
        if detected_indian and detected_indian != "mixed":
            translit_result = _try_transliterate_and_translate(
                text, target_language, detected_indian
            )
            if translit_result:
                return {
                    "translated_text": translit_result["translated"],
                    "detected_language": _lang_code_to_name(translit_result["lang_code"]),
                }

        # Try transliteration across multiple Indian languages
        best = _try_transliterate_and_translate(text, target_language, source)
        if best:
            return {
                "translated_text": best["translated"],
                "detected_language": _lang_code_to_name(best["lang_code"]),
            }

        # Return whatever Google gave us (even if similar to original)
        detected_lang = _detect_language_name(text)
        return {"translated_text": translated or text, "detected_language": detected_lang}

    except Exception:
        return {"translated_text": f"[Translation failed] {text}", "detected_language": "unknown"}


def _translate_with_pytrans(text, target_language, engine="google"):
    """Translate using py-trans library (Google, MyMemory, or translate.com).

    py-trans uses a different Google Translate endpoint than deep-translator,
    so it can sometimes produce different (better) auto-detection results.

    Args:
        text: Text to translate.
        target_language: Target language code (e.g. 'en').
        engine: One of 'google', 'my_memory', 'translate_com'.

    Returns:
        dict with 'translation' and 'origin_lang' keys, or None if failed.
    """
    try:
        from py_trans import PyTranslator
        tr = PyTranslator()

        if engine == "google":
            result = tr.google(text, target_language)
        elif engine == "my_memory":
            result = tr.my_memory(text, target_language)
        elif engine == "translate_com":
            result = tr.translate_com(text, target_language)
        else:
            return None

        if (
            result
            and result.get("status") == "success"
            and result.get("translation")
            and not _texts_are_similar(text, result["translation"])
        ):
            return result
    except Exception as e:
        logger.debug("py-trans %s failed for '%s': %s", engine, text[:60], e)
    return None


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
    "irukken", "irukkanga", "iruku", "iruka", "irukka", "irukaa",
    "aana", "aanalum", "aanaa",
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
    # Common YouTube comment words in Tanglish
    "padam", "padama", "pathu", "paarunga", "subscribe", "pannunga",
    "vera", "level", "thalaiva", "thalaivan", "thalaivar",
    "nanba", "tamizh", "tamil", "ennaku", "pidikum", "pidikkum",
    "kovam", "santhosham", "bayam", "kashtam", "kastam",
    "seri", "serigaa", "theriyum", "theriyala", "sollu", "sollunga",
    "paakalam", "ketpom", "kelunga", "varum", "pogum",
    "nallavanga", "kettavanga", "periya", "chinna", "pudhusu",
    "eppadi", "yeppadi", "yenna", "yaen", "yean", "yen",
    "pakkathula", "nimmathiya", "podhum", "konjam", "niraiya",
    # Question forms and conversational words
    "eppadi", "eppdi", "eppo", "evlo", "enga", "ethuku",
    "pannuva", "pannuvanga", "varuva", "varuvanga", "irukkum",
    "sollunga", "solluga", "pannuga", "parunga", "kelunga",
    "theriyuma", "puriyuma", "mudiyuma", "kedaikuma",
    "pannalaam", "pogalaam", "varalaam", "sollalaam",
    "theriyadha", "puriyaadha", "mudiyaadha",
    "pannirukkanga", "vandhirukkanga", "sollirukkanga",
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

    non_english_count = 0
    for w in clean_words:
        if w in _COMMON_ENGLISH:
            continue
        non_english_count += 1
        if w in _TANGLISH_WORDS:
            tamil_score += 1
        if w in _HINGLISH_WORDS:
            hindi_score += 1
        # Check suffixes for words not in dictionaries
        if len(w) > 2:
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

    # If there are non-English words but no specific language detected,
    # return "mixed" to signal Claude should handle it
    if non_english_count >= 2 and len(clean_words) > 2:
        english_ratio = sum(1 for w in clean_words if w in _COMMON_ENGLISH) / len(clean_words)
        if english_ratio < 0.8:
            return "mixed"

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
        "pa": "Punjabi", "ur": "Urdu", "mixed": "an Indian language",
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
                        f"Translate this social media comment to {target_name}.\n\n"
                        "The text is likely Tanglish (Tamil+English) or Hinglish (Hindi+English) "
                        "where Indian language words are written in English/Latin letters, "
                        "often mixed with actual English words in the same sentence.\n\n"
                        "TANGLISH EXAMPLES:\n"
                        "- 'bro anga placements la iruka?' = 'Bro, are there placements there?'\n"
                        "- 'nalla irukku' = 'It is good'\n"
                        "- 'semma video da' = 'Great video man'\n"
                        "- 'enna price bro' = 'What is the price bro?'\n"
                        "- 'bro eppo release pannuva' = 'Bro when will you release it?'\n"
                        "- 'romba nalla explain pannirukeenga' = 'You explained very well'\n"
                        "- 'subscribe pannunga friends' = 'Subscribe friends'\n\n"
                        "HINGLISH EXAMPLES:\n"
                        "- 'bahut accha hai bhai' = 'It is very good brother'\n"
                        "- 'ye kab aayega' = 'When will this come?'\n"
                        "- 'price kitna hai' = 'What is the price?'\n\n"
                        "RULES:\n"
                        "1. Keep English words (placements, video, subscribe, etc.) as-is in translation\n"
                        "2. Translate the MEANING of regional words, not their English spelling\n"
                        "3. Produce natural, fluent English — not word-by-word\n"
                        f"{hint}\n\n"
                        "Respond with ONLY a JSON object:\n"
                        f'{{"translated_text": "accurate {target_name} translation", '
                        '"detected_language": "tanglish" or "hinglish" or language name}\n\n'
                        f"Text: {text}"
                    ),
                }
            ],
        )
        raw = message.content[0].text.strip()
        logger.info("Claude translate raw response: %s", raw[:200])
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
        logger.warning("Claude translation rejected (too similar): '%s' vs '%s'", text[:80], translated[:80])
    except Exception as e:
        logger.error("Claude translation failed for '%s': %s", text[:80], e)
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
            native_text = _transliterate_aggressive(text, lang_code)
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


def _is_known_regional_word(word, lang_code):
    """Check if a word is a known Tamil/Hindi word that should be transliterated."""
    clean = word.rstrip(".,!?;:'\"").lower()
    if clean in _COMMON_ENGLISH:
        return False
    if lang_code == "ta" or lang_code is None:
        if clean in _TANGLISH_WORDS:
            return True
        if len(clean) > 2 and any(clean.endswith(s) for s in _TAMIL_SUFFIXES):
            return True
    if lang_code == "hi" or lang_code is None:
        if clean in _HINGLISH_WORDS:
            return True
        if len(clean) > 2 and any(clean.endswith(s) for s in _HINDI_SUFFIXES):
            return True
    return False


def _transliterate_to_native(text, lang_code):
    """Convert Latin-script text to native script using Google Input Tools.

    Only transliterates words that are known Tamil/Hindi words (from our
    dictionaries or suffix patterns). All other words — including English
    words NOT in _COMMON_ENGLISH like "placements", "college" — are kept
    as-is to prevent Google Translate from corrupting them.
    """
    import requests as _req
    try:
        words = text.split()
        result_words = []
        for word in words:
            clean = word.rstrip(".,!?;:'\"").lower()
            # Only transliterate words we're confident are regional language
            if not _is_known_regional_word(word, lang_code):
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


_ENGLISH_SUFFIXES = (
    "ment", "ments", "tion", "tions", "sion", "sions", "ness", "ness",
    "able", "ible", "ful", "less", "ous", "ious", "ive", "ical",
    "ing", "ings", "ated", "tion", "ally", "ment", "ence", "ance",
    "ship", "ward", "wise", "like", "ology", "ular", "ity",
)


def _looks_like_english_word(word):
    """Check if a word looks like an English word based on morphology."""
    w = word.lower()
    if w in _COMMON_ENGLISH:
        return True
    # English morphological patterns (plurals, past tense, etc.)
    if any(w.endswith(s) for s in _ENGLISH_SUFFIXES):
        return True
    # Common English plural/past forms
    if len(w) > 3 and w.endswith("s") and w[:-1] in _COMMON_ENGLISH:
        return True
    if len(w) > 3 and w.endswith("ed") and w[:-2] in _COMMON_ENGLISH:
        return True
    if len(w) > 4 and w.endswith("ing") and w[:-3] in _COMMON_ENGLISH:
        return True
    return False


def _transliterate_aggressive(text, lang_code="ta"):
    """Aggressively transliterate ALL non-English words to native script.

    Unlike _transliterate_to_native() which only converts known dictionary
    words, this sends every non-English word through Google Input Tools.
    This handles Tanglish words missing from our dictionary like 'vazhkaila',
    'padichirukken', etc.
    """
    import requests as _req
    try:
        words = text.split()
        result_words = []
        for word in words:
            clean = word.rstrip(".,!?;:'\"").lower()
            # Skip English words (common set + morphological patterns)
            if _looks_like_english_word(clean) or len(clean) <= 1:
                result_words.append(word)
                continue
            try:
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
                    timeout=3,
                )
                data = resp.json()
                if data[0] == "SUCCESS" and data[1]:
                    candidates = data[1][0][1]
                    native_word = candidates[0] if candidates else word
                    # Only use if it actually produced non-Latin characters
                    if any(not c.isascii() for c in native_word if c.isalpha()):
                        result_words.append(native_word)
                    else:
                        result_words.append(word)
                else:
                    result_words.append(word)
            except Exception:
                result_words.append(word)
        return " ".join(result_words)
    except Exception:
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
    """NLP ensemble sentiment analysis: VADER + TextBlob + regional keywords.

    Pipeline:
    1. Lead detection (contact info patterns).
    2. VADER on ORIGINAL text (preserves capitalization, punctuation, emojis).
    3. NLP preprocess (tokenize + lemmatize + stop-word removal) → TextBlob.
    4. Regional keyword scoring (Tanglish/Hinglish dictionaries).
    5. Emoji signal scoring.
    6. For non-English with weak signal: translate → re-run VADER + TextBlob.
    7. Weighted ensemble: VADER 0.40, TextBlob 0.25, Regional 0.20, Emoji 0.15.
    """
    if not text or not text.strip():
        return {"sentiment": "neutral", "score": 0.5}

    orig_lower = text.lower()

    # ── Step 0: Lead detection ──
    lead_phrases = {"call me", "contact me", "my number", "my email", "reach me", "phone"}
    for phrase in lead_phrases:
        if phrase in orig_lower:
            return {"sentiment": "lead", "score": 0.8}

    # ── Step 1: VADER on ORIGINAL text ──
    vader = _get_vader()
    vader_compound = 0.0
    vader_available = False
    if vader:
        scores = vader.polarity_scores(text)
        vader_compound = scores["compound"]
        vader_available = True

    # ── Step 2: TextBlob on preprocessed text ──
    preprocessed = _nlp_preprocess(text)
    tb_polarity, tb_subjectivity = _textblob_sentiment(preprocessed)

    # ── Step 3: Regional keyword scoring (Tanglish/Hinglish) ──
    tanglish_positive = {
        "nalla", "nallaa", "super", "semma", "mass", "gethu",
        "arumai", "azhaga", "azhagaa", "nandri", "romba nalla",
        "kalakkal", "adipoli", "theri", "vera level", "vera maari",
        "pidikum", "pidikkum", "pidichirukku", "azhaku",
        "magizhchi", "santhosham", "mikka nandri", "nalla irukku",
        "sirantha", "peruma", "perumaya", "mikavum",
        "nalla panreenga", "nalla solreenga",
    }
    tanglish_negative = {
        "mokka", "kaduppu", "kovam", "ketta", "kettadhu",
        "mosam", "mosamaa", "bayam", "kashtam", "kastam",
        "kedaikala", "pudikkala", "pudikala", "venam", "venda",
        "asingam", "asingama", "mayiru", "mairu", "thevai illa",
        "thalai vali", "bore", "boredhu", "mosam pannunga",
        "olunga", "olungaa", "mokka video", "koluthi podu",
        "onnum illa", "waste pannaadha", "use illa",
    }
    hinglish_positive = {
        "accha", "achha", "bahut accha", "mast", "zabardast",
        "kamaal", "shandaar", "shaandar", "behtareen", "pyaara",
        "sundar", "badhiya", "jhakaas", "waah", "sahi hai",
        "bohot acha", "dil khush", "pasand aaya", "tagda",
    }
    hinglish_negative = {
        "bakwas", "bekar", "ghatiya", "wahiyat", "ganda",
        "galat", "bura", "kharab", "tatti", "faltu",
        "dhoka", "jhooth", "jhootha", "paisa barbaad",
        "time waste", "bekaar", "nautanki", "bakwaas",
    }

    regional_pos = sum(1 for w in tanglish_positive if w in orig_lower)
    regional_pos += sum(1 for w in hinglish_positive if w in orig_lower)
    regional_neg = sum(1 for w in tanglish_negative if w in orig_lower)
    regional_neg += sum(1 for w in hinglish_negative if w in orig_lower)
    regional_signal = (regional_pos - regional_neg) * 0.30

    # ── Step 4: Emoji signal ──
    emoji_pos = sum(1 for ch in text if ch in _POSITIVE_EMOJIS)
    emoji_neg = sum(1 for ch in text if ch in _NEGATIVE_EMOJIS)
    emoji_signal = (emoji_pos - emoji_neg) * 0.1

    # ── Step 5: Translate + re-score for non-English with weak signals ──
    translated_vader = 0.0
    translated_tb = 0.0
    has_translation = False
    if not skip_translate and vader_available:
        primary_signal = abs(vader_compound) + abs(tb_polarity)
        if primary_signal < 0.5:
            is_non_english = not _is_latin_script(text)
            is_mixed_lang = _detect_indian_language(text) is not None
            if is_non_english or is_mixed_lang:
                try:
                    from deep_translator import GoogleTranslator
                    translated = GoogleTranslator(
                        source="auto", target="en"
                    ).translate(text)
                    if translated and not _texts_are_similar(text, translated):
                        translated_vader = vader.polarity_scores(translated)["compound"]
                        trans_preprocessed = _nlp_preprocess(translated)
                        translated_tb, _ = _textblob_sentiment(trans_preprocessed)
                        has_translation = True
                except Exception:
                    pass

    # ── Step 6: Weighted ensemble ──
    if has_translation:
        effective_vader = (
            translated_vader
            if abs(translated_vader) > abs(vader_compound)
            else vader_compound
        )
        effective_tb = (
            translated_tb
            if abs(translated_tb) > abs(tb_polarity)
            else tb_polarity
        )
    else:
        effective_vader = vader_compound if vader_available else 0.0
        effective_tb = tb_polarity

    # Subjectivity boost: opinionated text → increase TextBlob weight
    tb_weight = 0.25
    vader_weight = 0.40
    if tb_subjectivity > 0.6:
        tb_weight = 0.30
        vader_weight = 0.35

    final_score = (
        effective_vader * vader_weight
        + effective_tb * tb_weight
        + regional_signal * 0.20
        + emoji_signal * 0.15
    )
    final_score = max(-1.0, min(1.0, final_score))

    # ── Step 7: Classify ──
    if final_score >= 0.05:
        confidence = 0.55 + (min(final_score, 1.0) - 0.05) * (0.4 / 0.95)
        return {"sentiment": "positive", "score": round(confidence, 2)}
    elif final_score <= -0.05:
        confidence = 0.45 - (abs(final_score) - 0.05) * (0.4 / 0.95)
        return {"sentiment": "negative", "score": round(confidence, 2)}
    else:
        return {"sentiment": "neutral", "score": 0.5}

"""Extract phone numbers and email addresses from comment text."""

import re


def extract_contacts(text):
    """Extract contact information from text.

    Returns:
        dict with 'emails' (list) and 'phones' (list).
    """
    if not text:
        return {"emails": [], "phones": []}

    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    phones = re.findall(
        r"(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text
    )

    return {"emails": list(set(emails)), "phones": list(set(phones))}


def has_contact_info(text):
    """Quick check whether text contains any contact information."""
    result = extract_contacts(text)
    return bool(result["emails"] or result["phones"])

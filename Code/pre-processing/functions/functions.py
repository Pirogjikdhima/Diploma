def normalize_quotes(text):
    """Normalize all types of quotation marks to a standard form"""
    replacements = {
        '"': '"',
        '“': '"',
        '”': '"',
        '‘': "'",
        '’': "'",
        "'": "'"
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text



def is_punctuation(text):
    return all(not c.isalnum() for c in text)

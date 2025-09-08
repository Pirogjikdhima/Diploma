from difflib import SequenceMatcher
import pandas as pd
import re

# --------------------------
# Helper functions
# --------------------------

def split_punct_tokens(tokens):
    """
    Split tokens that contain punctuation attached to words into separate tokens.
    e.g. '08:00' -> ['08', ':', '00'], 'word,' -> ['word', ',']
    Returns a new list of token dicts.
    """
    new_tokens = []
    for t in tokens:
        word = str(t['WORD'])
        # Split into alphanumeric and punctuation sequences
        parts = re.findall(r'\w+|[^\w\s]', word)
        if len(parts) == 1:
            new_tokens.append(t)
        else:
            for p in parts:
                new_t = t.copy()
                new_t['WORD'] = p
                new_tokens.append(new_t)
    return new_tokens

def normalize_for_matching(tokens):
    """Join tokens with spaces for fuzzy matching."""
    text = ' '.join([str(t['WORD']).strip() for t in tokens])
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def strip_punct(text):
    """Remove leading/trailing punctuation for fuzzy comparison."""
    return re.sub(r'^[^\w]+|[^\w]+$', '', text)

def fuzzy_ratio(a, b):
    return SequenceMatcher(None, strip_punct(a), strip_punct(b)).ratio()

# --------------------------
# Main DP alignment function
# --------------------------

def align_ner_to_pos_dp_split(ner_data, pos_data, threshold=0.8, max_span=1):
    """
    Align NER and POS tokens (after splitting punctuation) using DP with fuzzy matching.
    Returns a DataFrame.
    """
    # Split punctuation in both NER and POS
    ner_data_split = split_punct_tokens(ner_data)
    pos_data_split = split_punct_tokens(pos_data)

    n = len(ner_data_split)
    m = len(pos_data_split)

    # DP table and backtracking
    dp = [[0]*(m+1) for _ in range(n+1)]
    back = [[None]*(m+1) for _ in range(n+1)]

    # Fill DP table
    for i in range(n+1):
        for j in range(m+1):
            if i > 0 and dp[i][j] < dp[i-1][j]:
                dp[i][j] = dp[i-1][j]
                back[i][j] = (i-1, j)  # skip NER
            if j > 0 and dp[i][j] < dp[i][j-1]:
                dp[i][j] = dp[i][j-1]
                back[i][j] = (i, j-1)  # skip POS
            if i > 0 and j < m:
                # Try matching NER[i-1] to 1..max_span consecutive POS tokens
                for span in range(1, max_span+1):
                    if j+span > m:
                        break
                    candidate = normalize_for_matching(pos_data_split[j:j+span])
                    ner_word = str(ner_data_split[i-1]['WORD']).strip()
                    score = fuzzy_ratio(ner_word, candidate)
                    if score >= threshold:
                        match_score = dp[i-1][j] + 1
                        if match_score > dp[i][j+span-1]:
                            dp[i][j+span-1] = match_score
                            back[i][j+span-1] = (i-1, j, span)

    # Traceback
    i, j = n, m
    aligned_pairs = []
    unmatched_ner = []

    while i > 0 and j > 0:
        if back[i][j] is None:
            break
        prev = back[i][j]
        if len(prev) == 3:
            pi, pj, span = prev
            pos_tokens = pos_data_split[pj:pj+span]
            aligned_pairs.append((ner_data_split[pi], pos_tokens))
            i, j = pi, pj
        else:
            pi, pj = prev
            if pi == i-1 and pj == j:
                unmatched_ner.append(ner_data_split[i-1]['WORD'])
            i, j = pi, pj

    aligned_pairs.reverse()

    # Build DataFrame
    data = []
    for ner_token, pos_tokens in aligned_pairs:
        combined_word = ''.join([t['WORD'] for t in pos_tokens])
        combined = {
            "SENTENCE_ID": pos_tokens[-1].get("SENTENCE_ID"),
            "WORD": combined_word,
            "NER_TAG": ner_token["NER_TAG"],
            "POS_TAG": ' '.join([str(t['POS_TAG']) for t in pos_tokens]),
            "LEMMA": ' '.join([str(t['LEMMA']) for t in pos_tokens]),
            "FEATS": ' '.join([str(t['FEATS']) for t in pos_tokens]),
            "HEAD": ' '.join([str(t['HEAD']) for t in pos_tokens]),
            "DEPREL": ' '.join([str(t['DEPREL']) for t in pos_tokens]),
            "DEPS": ' '.join([str(t['DEPS']) for t in pos_tokens]),
            "MISC": ' '.join([str(t['MISC']) for t in pos_tokens]),
        }
        data.append(combined)

    if unmatched_ner:
        print(f"Unmatched NER tokens ({len(unmatched_ner)}):", unmatched_ner)

    return pd.DataFrame(data)

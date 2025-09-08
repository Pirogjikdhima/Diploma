from difflib import SequenceMatcher
import pandas as pd

def fuzzy_ratio(a, b):
    """Return similarity ratio between two strings."""
    return SequenceMatcher(None, a, b).ratio()

def align_ner_pos(ner_data, pos_data, threshold=0.8):
    """
    Align NER tokens to POS tokens using sequence alignment with fuzzy matching.
    Prints unmatched NER tokens.
    """
    ner_words = [t["WORD"] for t in ner_data]
    pos_words = [t["WORD"] for t in pos_data]

    n = len(ner_words)
    m = len(pos_words)

    # Initialize DP table
    dp = [[0]*(m+1) for _ in range(n+1)]
    back = [[None]*(m+1) for _ in range(n+1)]

    # Fill DP table
    for i in range(n+1):
        for j in range(m+1):
            if i > 0 and j > 0:
                score = fuzzy_ratio(ner_words[i-1], pos_words[j-1])
                match_score = dp[i-1][j-1] + (1 if score >= threshold else 0)
                dp[i][j] = match_score
                back[i][j] = (i-1, j-1)
            if i > 0 and (dp[i][j] < dp[i-1][j]):
                dp[i][j] = dp[i-1][j]
                back[i][j] = (i-1, j)
            if j > 0 and (dp[i][j] < dp[i][j-1]):
                dp[i][j] = dp[i][j-1]
                back[i][j] = (i, j-1)

    # Traceback
    i, j = n, m
    aligned_pairs = []
    matched_ner_indices = set()

    while i > 0 and j > 0:
        prev = back[i][j]
        if prev is None:
            break
        pi, pj = prev
        if pi == i-1 and pj == j-1:
            # Diagonal -> matched
            if fuzzy_ratio(ner_words[i-1], pos_words[j-1]) >= threshold:
                aligned_pairs.append((ner_data[i-1], pos_data[j-1]))
                matched_ner_indices.add(i-1)
        # Move to previous cell
        i, j = pi, pj

    aligned_pairs.reverse()  # traceback goes backwards

    # Report unmatched NER tokens
    unmatched_ner = [ner_data[idx]["WORD"] for idx in range(n) if idx not in matched_ner_indices]
    if unmatched_ner:
        print(f"Unmatched NER tokens ({len(unmatched_ner)}):", unmatched_ner)

    # Build DataFrame
    data = []
    for ner_token, pos_token in aligned_pairs:
        combined = {
            # "SENTENCE_ID": pos_token.get("SENTENCE_ID"),
            "WORD": pos_token["WORD"],
            "NER_TAG": ner_token["NER_TAG"],
            "POS_TAG": pos_token["POS_TAG"],
            "LEMMA": pos_token["LEMMA"],
            "FEATS": pos_token["FEATS"],
            "HEAD": pos_token["HEAD"],
            "DEPREL": pos_token["DEPREL"],
            "DEPS": pos_token["DEPS"],
            "MISC": pos_token["MISC"],
        }
        data.append(combined)

    return pd.DataFrame(data)

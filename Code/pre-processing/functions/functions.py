import csv
import multiprocessing as mp
from pathlib import Path

import rapidfuzz.process
from conllu import parse
from tqdm import tqdm


def normalize_quotes(text):
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

def parse_ner_file(path):
    ner_words = []
    ner_dict = {}

    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            parts = [normalize_quotes(part.strip()) for part in line.split("\t") if part.strip()]

            if len(parts) >= 2:
                word_info = parts[:2]
                ner_words.append(word_info)

                ner_dict[word_info[0]] = word_info

    return ner_words, ner_dict


def process_conllu_file(file_path):
    try:
        with Path(file_path).open(encoding="utf-8") as f:
            data = f.read()

        pos_words = []
        sentences = parse(data)
        for sentence in sentences:
            for token in sentence:
                word = normalize_quotes(token['form'])
                pos_tokens = [
                    word,
                    normalize_quotes(token['lemma']),
                    token['upostag'],
                    token['feats'],
                    token['head'],
                    token['deprel'],
                    token['deps'],
                    token['misc'],
                ]
                pos_words.append(pos_tokens)

        return pos_words
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []


def get_all_conllu_files(conllu_dir):
    conllu_files = []

    for i in range(1, 10):
        subdir = conllu_dir / f"{i}Part"
        if subdir.exists() and subdir.is_dir():
            subdir_files = list(subdir.glob("*.conllu"))
            conllu_files.extend(subdir_files)
            print(f"Found {len(subdir_files)} CONLLU files in {subdir}")

    return conllu_files


def process_conllu_files_parallel(conllu_dir):
    file_paths = get_all_conllu_files(conllu_dir)

    if not file_paths:
        print(f"Warning: No CONLLU files found in subdirectories of {conllu_dir}")
        return [], {}, {}

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = list(tqdm(
            pool.imap(process_conllu_file, file_paths),
            total=len(file_paths),
            desc="Processing CONLLU files"
        ))

    pos_words = [item for sublist in results for item in sublist]

    pos_dict = {}
    for entry in pos_words:
        pos_dict[entry[0]] = entry

    quotes_dict = {
        '"': next((e for e in pos_words if e[0] in ['"', '“', '”']), None),
        "'": next((e for e in pos_words if e[0] in ["'", '‘', '’']), None)
    }

    return pos_words, pos_dict, quotes_dict


# def match_ner_with_pos(ner_words, pos_dict, quotes_dict, threshold=80, output_file="combined_words.conllu",
#                        unmatched_file="unmatched_ner.txt"):
#     """Match NER words with POS words efficiently and write results in CoNLL-U format"""
#     # Open files for writing
#     with open(output_file, "w", encoding="utf-8") as f_combined, open(unmatched_file, "w",
#                                                                       encoding="utf-8") as f_unmatched:
#
#         # For stats tracking
#         matched_count = 0
#         quotes_matched = 0
#         quotes_unmatched = 0
#         current_sentence_id = 1
#
#         # Write CoNLL-U header
#         f_combined.write("# newdoc\n")
#         f_combined.write(f"# sent_id = {current_sentence_id}\n")
#         f_combined.write("# text = Generated combined NER and POS data\n")
#
#         # For fuzzy matching, prepare a list of words once
#         pos_word_list = list(pos_dict.keys())
#         token_id = 1
#
#         for ner_entry in tqdm(ner_words, desc="Matching NER with POS"):
#             ner_word = ner_entry[0]
#             ner_tag = ner_entry[1] if len(ner_entry) > 1 else "_"
#             matched = False
#
#             # Skip empty words
#             if not ner_word.strip():
#                 continue
#
#             if ner_word == "...":
#                 line = f"{ner_word}\t\t{ner_tag}\t...\tPUNCT\t_"
#                 f_combined.write(f"{line}\n")
#                 matched = True
#
#             # Direct match (most efficient)
#             elif ner_word in pos_dict:
#                 pos_info = pos_dict[ner_word]
#                 # Format: WORD    NER_TAG    LEMMA    UPOS    FEATS    HEAD    DEPREL    DEPS    MISC
#                 lemma = pos_info[1] if len(pos_info) > 1 and pos_info[1] else "_"
#                 upos = pos_info[2] if len(pos_info) > 2 and pos_info[2] else "_"
#                 feats = pos_info[3] if len(pos_info) > 3 and pos_info[3] else "_"
#                 head = pos_info[4] if len(pos_info) > 4 and pos_info[4] else "_"
#                 deprel = pos_info[5] if len(pos_info) > 5 and pos_info[5] else "_"
#                 deps = pos_info[6] if len(pos_info) > 6 and pos_info[6] else "_"
#                 misc = pos_info[7] if len(pos_info) > 7 and pos_info[7] else "_"
#
#                 # Write in simple tab-separated format
#                 line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}\t{head}\t{deprel}\t{deps}\t{misc}"
#                 f_combined.write(f"{line}\n")
#                 matched = True
#
#             else:
#                 if not is_punctuation(ner_word):
#                     try:
#                         matches = rapidfuzz.process.extractOne(
#                             ner_word,
#                             pos_word_list,
#                             score_cutoff=threshold
#                         )
#
#                         if matches:
#                             best_match, score, _ = matches
#                             pos_info = pos_dict[best_match]
#                             lemma = pos_info[1] if len(pos_info) > 1 else "_"
#                             upos = pos_info[2] if len(pos_info) > 2 else "_"
#                             feats = pos_info[3] if len(pos_info) > 3 else "_"
#                             head = pos_info[4] if len(pos_info) > 4 else "_"
#                             deprel = pos_info[5] if len(pos_info) > 5 else "_"
#                             deps = pos_info[6] if len(pos_info) > 6 else "_"
#                             misc = pos_info[7] if len(pos_info) > 7 else "_"
#
#                             conllu_line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}\t{head}\t{deprel}\t{deps}\t{misc}"
#                             f_combined.write(f"{conllu_line}\n")
#                             matched = True
#                     except Exception as e:
#                         print(f"Error matching '{ner_word}': {e}")
#
#             if not matched:
#                 f_unmatched.write(f"{ner_word}\t{ner_tag}\n")
#                 if ner_word in ['"', '“', '”', "'", '‘', '’']:
#                     quotes_unmatched += 1
#             else:
#                 matched_count += 1
#                 token_id += 1
#
#     return {
#         'matched_count': matched_count,
#         'unmatched_count': len(ner_words) - matched_count,
#         'quotes_matched': quotes_matched,
#         'quotes_unmatched': quotes_unmatched,
#         'total_ner_words': len(ner_words)
#     }

def match_ner_with_pos_array(ner_words, pos_words, threshold=80, output_file="combined_words.conllu",
                             unmatched_file="unmatched_ner.txt"):
    """Match NER words with POS words using array to preserve order and all values"""

    # Create a lookup structure that preserves all occurrences
    # This will be a dictionary where keys are words and values are lists of all their POS entries
    pos_lookup = {}
    pos_word_list = []  # For fuzzy matching

    for pos_entry in pos_words:
        word = pos_entry[0]
        if word not in pos_lookup:
            pos_lookup[word] = []
            pos_word_list.append(word)  # Only add unique words for fuzzy matching
        pos_lookup[word].append(pos_entry)

    # Open files for writing
    with open(output_file, "w", encoding="utf-8") as f_combined, \
            open(unmatched_file, "w", encoding="utf-8") as f_unmatched:

        # For stats tracking
        matched_count = 0
        current_sentence_id = 1

        # Write CoNLL-U header
        f_combined.write("# newdoc\n")
        f_combined.write(f"# sent_id = {current_sentence_id}\n")
        f_combined.write("# text = Generated combined NER and POS data\n")

        for ner_entry in tqdm(ner_words, desc="Matching NER with POS"):
            ner_word = ner_entry[0]
            ner_tag = ner_entry[1] if len(ner_entry) > 1 else "_"
            matched = False

            # Skip empty words
            if not ner_word.strip():
                continue

            if ner_word == "...":
                line = f"{ner_word}\t\t{ner_tag}\t...\tPUNCT\t_"
                f_combined.write(f"{line}\n")
                matched = True

            # Direct match - use the first occurrence (or you could use a different strategy)
            elif ner_word in pos_lookup:
                # Get the first POS entry for this word (you can modify this logic as needed)
                pos_info = pos_lookup[ner_word][0]  # First occurrence

                # If you want to use all occurrences, you could iterate through them:
                # for pos_info in pos_lookup[ner_word]:
                #     # Write each occurrence

                lemma = pos_info[1] if len(pos_info) > 1 and pos_info[1] else "_"
                upos = pos_info[2] if len(pos_info) > 2 and pos_info[2] else "_"
                feats = pos_info[3] if len(pos_info) > 3 and pos_info[3] else "_"
                head = pos_info[4] if len(pos_info) > 4 and pos_info[4] else "_"
                deprel = pos_info[5] if len(pos_info) > 5 and pos_info[5] else "_"
                deps = pos_info[6] if len(pos_info) > 6 and pos_info[6] else "_"
                misc = pos_info[7] if len(pos_info) > 7 and pos_info[7] else "_"

                line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}\t{head}\t{deprel}\t{deps}\t{misc}"
                f_combined.write(f"{line}\n")
                matched = True

            else:
                # Fuzzy matching for non-punctuation words
                if not is_punctuation(ner_word):
                    try:
                        matches = rapidfuzz.process.extractOne(
                            ner_word,
                            pos_word_list,
                            score_cutoff=threshold
                        )

                        if matches:
                            best_match, score, _ = matches
                            # Use the first occurrence of the best match
                            pos_info = pos_lookup[best_match][0]

                            lemma = pos_info[1] if len(pos_info) > 1 else "_"
                            upos = pos_info[2] if len(pos_info) > 2 else "_"
                            feats = pos_info[3] if len(pos_info) > 3 else "_"
                            head = pos_info[4] if len(pos_info) > 4 else "_"
                            deprel = pos_info[5] if len(pos_info) > 5 else "_"
                            deps = pos_info[6] if len(pos_info) > 6 else "_"
                            misc = pos_info[7] if len(pos_info) > 7 else "_"

                            conllu_line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}\t{head}\t{deprel}\t{deps}\t{misc}"
                            f_combined.write(f"{conllu_line}\n")
                            matched = True
                    except Exception as e:
                        print(f"Error matching '{ner_word}': {e}")

            if not matched:
                f_unmatched.write(f"{ner_word}\t{ner_tag}\n")
            else:
                matched_count += 1

    return {
        'matched_count': matched_count,
        'unmatched_count': len(ner_words) - matched_count,
        'total_ner_words': len(ner_words)
    }

# def match_ner_with_pos(ner_words, pos_dict, quotes_dict=None, threshold=80,
#                        output_file="combined_words.csv", unmatched_file="unmatched_ner.txt"):
#     """Match NER words with POS words efficiently and write results in CSV format"""
#
#     # For stats tracking
#     matched_count = 0
#     quotes_matched = 0
#     quotes_unmatched = 0
#
#     # For fuzzy matching, prepare a list of words once
#     pos_word_list = list(pos_dict.keys())
#
#     # Open files for writing
#     with open(output_file, "w", newline="", encoding="utf-8") as f_combined, \
#             open(unmatched_file, "w", encoding="utf-8") as f_unmatched:
#
#         # Create CSV writer
#         csv_writer = csv.writer(f_combined)
#
#         # Write CSV header
#         csv_writer.writerow(["word", "ner", "pos"])
#
#         for ner_entry in tqdm(ner_words, desc="Matching NER with POS"):
#             ner_word = ner_entry[0]
#             ner_tag = ner_entry[1] if len(ner_entry) > 1 else "_"
#             matched = False
#             pos_info = "_"
#
#             # Skip empty words
#             if not ner_word.strip():
#                 continue
#
#             # Handle ellipsis
#             if ner_word == "...":
#                 pos_info = "PUNCT"
#                 csv_writer.writerow([ner_word, ner_tag, pos_info])
#                 matched = True
#
#             # Direct match (most efficient)
#             elif ner_word in pos_dict:
#                 pos_info = pos_dict[ner_word]
#                 # Convert the entire pos_info to string, handling different data types
#                 if isinstance(pos_info, (list, tuple)):
#                     pos_string = "|".join(str(item) for item in pos_info if item and str(item) != "_")
#                 else:
#                     pos_string = str(pos_info)
#                 csv_writer.writerow([ner_word, ner_tag, pos_string])
#                 matched = True
#
#             else:
#                 # Try fuzzy matching for non-punctuation words
#                 if not is_punctuation(ner_word):
#                     try:
#                         matches = rapidfuzz.process.extractOne(
#                             ner_word,
#                             pos_word_list,
#                             score_cutoff=threshold
#                         )
#
#                         if matches:
#                             best_match, score, _ = matches
#                             pos_info = pos_dict[best_match]
#                             # Convert the entire pos_info to string
#                             if isinstance(pos_info, (list, tuple)):
#                                 pos_string = "|".join(str(item) for item in pos_info if item and str(item) != "_")
#                             else:
#                                 pos_string = str(pos_info)
#                             csv_writer.writerow([ner_word, ner_tag, pos_string])
#                             matched = True
#                     except Exception as e:
#                         print(f"Error matching '{ner_word}': {e}")
#
#             # Handle unmatched words
#             if not matched:
#                 f_unmatched.write(f"{ner_word}\t{ner_tag}\n")
#                 if ner_word in ['"', '"', '"', "'", ''', ''']:
#                     quotes_unmatched += 1
#             else:
#                 matched_count += 1
#
#     return {
#         'matched_count': matched_count,
#         'unmatched_count': len(ner_words) - matched_count,
#         'quotes_matched': quotes_matched,
#         'quotes_unmatched': quotes_unmatched,
#         'total_ner_words': len(ner_words)
#     }
def match_ner_with_pos_strict_sequential(ner_words, pos_words, threshold=80, output_file="combined_words.conllu",
                                         unmatched_file="unmatched_ner.txt"):
    """
    Alternative approach: Match words in strict sequential order
    This assumes that the order of words in NER roughly matches the order in POS
    """

    # Create index tracker for pos_words
    pos_index = 0
    pos_word_list = [entry[0] for entry in pos_words]  # For fuzzy matching

    # Open files for writing
    with open(output_file, "w", encoding="utf-8") as f_combined, \
            open(unmatched_file, "w", encoding="utf-8") as f_unmatched:

        matched_count = 0

        # Write CoNLL-U header
        f_combined.write("# newdoc\n")
        f_combined.write("# sent_id = 1\n")
        f_combined.write("# text = Generated combined NER and POS data\n")

        for ner_entry in tqdm(ner_words, desc="Matching NER with POS sequentially"):
            ner_word = ner_entry[0]
            ner_tag = ner_entry[1] if len(ner_entry) > 1 else "_"
            matched = False

            # Skip empty words
            if not ner_word.strip():
                continue

            if ner_word == "...":
                line = f"{ner_word}\t\t{ner_tag}\t...\tPUNCT\t_"
                f_combined.write(f"{line}\n")
                matched = True

            else:
                # Look for the word starting from current pos_index
                found_index = -1

                # Search forward in pos_words for this ner_word
                for i in range(pos_index, len(pos_words)):
                    if pos_words[i][0] == ner_word:
                        found_index = i
                        break

                # If not found, try fuzzy matching from current position
                if found_index == -1 and not is_punctuation(ner_word):
                    try:
                        # Search in a window around current position for efficiency
                        window_start = max(0, pos_index - 50)
                        window_end = min(len(pos_words), pos_index + 100)
                        search_words = pos_word_list[window_start:window_end]

                        matches = rapidfuzz.process.extractOne(
                            ner_word,
                            search_words,
                            score_cutoff=threshold
                        )

                        if matches:
                            best_match, score, _ = matches
                            # Find the actual index in the full array
                            for i in range(window_start, window_end):
                                if pos_words[i][0] == best_match:
                                    found_index = i
                                    break
                    except Exception as e:
                        print(f"Error matching '{ner_word}': {e}")

                # If we found a match, use it
                if found_index != -1:
                    pos_info = pos_words[found_index]
                    pos_index = found_index + 1  # Move to next position

                    lemma = pos_info[1] if len(pos_info) > 1 and pos_info[1] else "_"
                    upos = pos_info[2] if len(pos_info) > 2 and pos_info[2] else "_"
                    feats = pos_info[3] if len(pos_info) > 3 and pos_info[3] else "_"
                    head = pos_info[4] if len(pos_info) > 4 and pos_info[4] else "_"
                    deprel = pos_info[5] if len(pos_info) > 5 and pos_info[5] else "_"
                    deps = pos_info[6] if len(pos_info) > 6 and pos_info[6] else "_"
                    misc = pos_info[7] if len(pos_info) > 7 and pos_info[7] else "_"

                    line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}\t{head}\t{deprel}\t{deps}\t{misc}"
                    f_combined.write(f"{line}\n")
                    matched = True

            if not matched:
                f_unmatched.write(f"{ner_word}\t{ner_tag}\n")
            else:
                matched_count += 1

    return {
        'matched_count': matched_count,
        'unmatched_count': len(ner_words) - matched_count,
        'total_ner_words': len(ner_words),
        'final_pos_index': pos_index
    }


def match_ner_with_pos_sequential(ner_words, pos_words, threshold=80, output_file="combined_words.conllu",
                                  unmatched_file="unmatched_ner.txt"):
    """Match NER words with POS words preserving sequential order from POS array"""

    # Create a usage tracker for each word in pos_words
    # This keeps track of how many times we've used each word
    word_usage_count = {}

    # Create a lookup that groups POS entries by word in order
    pos_lookup_ordered = {}
    pos_word_list = []  # For fuzzy matching

    for pos_entry in pos_words:
        word = pos_entry[0]
        if word not in pos_lookup_ordered:
            pos_lookup_ordered[word] = []
            pos_word_list.append(word)  # Only add unique words for fuzzy matching
            word_usage_count[word] = 0
        pos_lookup_ordered[word].append(pos_entry)

    # Open files for writing
    with open(output_file, "w", encoding="utf-8") as f_combined, \
            open(unmatched_file, "w", encoding="utf-8") as f_unmatched:

        # For stats tracking
        matched_count = 0
        current_sentence_id = 1

        # Write CoNLL-U header
        f_combined.write("# newdoc\n")
        f_combined.write(f"# sent_id = {current_sentence_id}\n")
        f_combined.write("# text = Generated combined NER and POS data\n")

        for ner_entry in tqdm(ner_words, desc="Matching NER with POS"):
            ner_word = ner_entry[0]
            ner_tag = ner_entry[1] if len(ner_entry) > 1 else "_"
            matched = False

            # Skip empty words
            if not ner_word.strip():
                continue

            if ner_word == "...":
                line = f"{ner_word}\t\t{ner_tag}\t...\tPUNCT\t_"
                f_combined.write(f"{line}\n")
                matched = True

            # Direct match - use sequential occurrence
            elif ner_word in pos_lookup_ordered:
                # Get the current usage count for this word
                current_usage = word_usage_count[ner_word]

                # If we still have unused occurrences of this word
                if current_usage < len(pos_lookup_ordered[ner_word]):
                    pos_info = pos_lookup_ordered[ner_word][current_usage]
                    word_usage_count[ner_word] += 1  # Increment usage count
                else:
                    # If we've used all occurrences, cycle back to the first one
                    # Or you could use the last one, or handle this differently
                    pos_info = pos_lookup_ordered[ner_word][0]

                lemma = pos_info[1] if len(pos_info) > 1 and pos_info[1] else "_"
                upos = pos_info[2] if len(pos_info) > 2 and pos_info[2] else "_"
                feats = pos_info[3] if len(pos_info) > 3 and pos_info[3] else "_"
                head = pos_info[4] if len(pos_info) > 4 and pos_info[4] else "_"
                deprel = pos_info[5] if len(pos_info) > 5 and pos_info[5] else "_"
                deps = pos_info[6] if len(pos_info) > 6 and pos_info[6] else "_"
                misc = pos_info[7] if len(pos_info) > 7 and pos_info[7] else "_"

                line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}\t{head}\t{deprel}\t{deps}\t{misc}"
                f_combined.write(f"{line}\n")
                matched = True

            else:
                # Fuzzy matching for non-punctuation words
                if not is_punctuation(ner_word):
                    try:
                        matches = rapidfuzz.process.extractOne(
                            ner_word,
                            pos_word_list,
                            score_cutoff=threshold
                        )

                        if matches:
                            best_match, score, _ = matches

                            # Use sequential occurrence for fuzzy matches too
                            current_usage = word_usage_count[best_match]
                            if current_usage < len(pos_lookup_ordered[best_match]):
                                pos_info = pos_lookup_ordered[best_match][current_usage]
                                word_usage_count[best_match] += 1
                            else:
                                pos_info = pos_lookup_ordered[best_match][0]

                            lemma = pos_info[1] if len(pos_info) > 1 else "_"
                            upos = pos_info[2] if len(pos_info) > 2 else "_"
                            feats = pos_info[3] if len(pos_info) > 3 else "_"
                            head = pos_info[4] if len(pos_info) > 4 else "_"
                            deprel = pos_info[5] if len(pos_info) > 5 else "_"
                            deps = pos_info[6] if len(pos_info) > 6 else "_"
                            misc = pos_info[7] if len(pos_info) > 7 else "_"

                            conllu_line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}\t{head}\t{deprel}\t{deps}\t{misc}"
                            f_combined.write(f"{conllu_line}\n")
                            matched = True
                    except Exception as e:
                        print(f"Error matching '{ner_word}': {e}")

            if not matched:
                f_unmatched.write(f"{ner_word}\t{ner_tag}\n")
            else:
                matched_count += 1

    return {
        'matched_count': matched_count,
        'unmatched_count': len(ner_words) - matched_count,
        'total_ner_words': len(ner_words),
        'word_usage_stats': word_usage_count  # Additional info about word usage
    }


def match_ner_with_pos_sequential_csv(ner_words, pos_words, threshold=80,
                                      output_file="combined_words.csv",
                                      unmatched_file="unmatched_ner.csv"):
    """Match NER words with POS words preserving sequential order from POS array and output to CSV"""

    # Create a usage tracker for each word in pos_words
    # This keeps track of how many times we've used each word
    word_usage_count = {}

    # Create a lookup that groups POS entries by word in order
    pos_lookup_ordered = {}
    pos_word_list = []  # For fuzzy matching

    for pos_entry in pos_words:
        word = pos_entry[0]
        if word not in pos_lookup_ordered:
            pos_lookup_ordered[word] = []
            pos_word_list.append(word)
            word_usage_count[word] = 0
        pos_lookup_ordered[word].append(pos_entry)

    csv_headers = ['word', 'ner_tag', 'lemma', 'upos', 'feats', 'head', 'deprel', 'deps', 'misc']
    unmatched_headers = ['word', 'ner_tag', 'reason']

    with open(output_file, "w", encoding="utf-8", newline='') as f_combined, \
            open(unmatched_file, "w", encoding="utf-8", newline='') as f_unmatched:

        csv_writer = csv.writer(f_combined)
        unmatched_writer = csv.writer(f_unmatched)

        csv_writer.writerow(csv_headers)
        unmatched_writer.writerow(unmatched_headers)

        matched_count = 0
        match_stats = {
            'direct_matches': 0,
            'fuzzy_matches': 0,
            'punctuation_matches': 0,
            'unmatched': 0
        }

        for ner_entry in tqdm(ner_words, desc="Matching NER with POS"):
            ner_word = ner_entry[0]
            ner_tag = ner_entry[1] if len(ner_entry) > 1 else "_"
            matched = False

            if not ner_word.strip():
                continue

            if ner_word == "...":
                row = [ner_word, ner_tag, "...", "PUNCT", "_", "_", "_", "_", "_"]
                csv_writer.writerow(row)
                matched = True
                match_stats['punctuation_matches'] += 1

            elif ner_word in pos_lookup_ordered:
                current_usage = word_usage_count[ner_word]

                if current_usage < len(pos_lookup_ordered[ner_word]):
                    pos_info = pos_lookup_ordered[ner_word][current_usage]
                    word_usage_count[ner_word] += 1
                else:
                    # If we've used all occurrences, cycle back to the first one
                    pos_info = pos_lookup_ordered[ner_word][0]

                lemma = pos_info[1] if len(pos_info) > 1 and pos_info[1] else "_"
                upos = pos_info[2] if len(pos_info) > 2 and pos_info[2] else "_"
                feats = pos_info[3] if len(pos_info) > 3 and pos_info[3] else "_"
                head = pos_info[4] if len(pos_info) > 4 and pos_info[4] else "_"
                deprel = pos_info[5] if len(pos_info) > 5 and pos_info[5] else "_"
                deps = pos_info[6] if len(pos_info) > 6 and pos_info[6] else "_"
                misc = pos_info[7] if len(pos_info) > 7 and pos_info[7] else "_"

                row = [ner_word, ner_tag, lemma, upos, feats, head, deprel, deps, misc]
                csv_writer.writerow(row)
                matched = True
                match_stats['direct_matches'] += 1

            else:
                if not is_punctuation(ner_word):
                    try:
                        matches = rapidfuzz.process.extractOne(
                            ner_word,
                            pos_word_list,
                            score_cutoff=threshold
                        )

                        if matches:
                            best_match, score, _ = matches

                            current_usage = word_usage_count[best_match]
                            if current_usage < len(pos_lookup_ordered[best_match]):
                                pos_info = pos_lookup_ordered[best_match][current_usage]
                                word_usage_count[best_match] += 1
                            else:
                                pos_info = pos_lookup_ordered[best_match][0]

                            lemma = pos_info[1] if len(pos_info) > 1 else "_"
                            upos = pos_info[2] if len(pos_info) > 2 else "_"
                            feats = pos_info[3] if len(pos_info) > 3 else "_"
                            head = pos_info[4] if len(pos_info) > 4 else "_"
                            deprel = pos_info[5] if len(pos_info) > 5 else "_"
                            deps = pos_info[6] if len(pos_info) > 6 else "_"
                            misc = pos_info[7] if len(pos_info) > 7 else "_"

                            row = [ner_word, ner_tag, lemma, upos, feats, head, deprel, deps, misc]
                            csv_writer.writerow(row)
                            matched = True
                            match_stats['fuzzy_matches'] += 1
                    except Exception as e:
                        print(f"Error matching '{ner_word}': {e}")

            if not matched:
                reason = "no_match_found"
                if is_punctuation(ner_word):
                    reason = "punctuation_no_pos"
                elif ner_word in pos_lookup_ordered:
                    reason = "pos_exhausted"

                unmatched_writer.writerow([ner_word, ner_tag, reason])
                match_stats['unmatched'] += 1
            else:
                matched_count += 1

    return {
        'matched_count': matched_count,
        'unmatched_count': len(ner_words) - matched_count,
        'total_ner_words': len(ner_words),
        'word_usage_stats': word_usage_count,
        'match_statistics': match_stats,
        'match_rate': (matched_count / len(ner_words)) * 100 if ner_words else 0
    }
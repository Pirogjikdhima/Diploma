# from conllu import parse
# from pathlib import Path
# import logging
# from collections import defaultdict
# import multiprocessing as mp
# from tqdm import tqdm  # For progress bar
# import rapidfuzz.process  # Much faster than fuzzywuzzy
#
# logging.getLogger().setLevel(logging.ERROR)
#
#
# def normalize_quotes(text):
#     """Normalize all types of quotation marks to a standard form"""
#     replacements = {
#         '"': '"',
#         '"': '"',
#         '"': '"',
#         ''': "'",
#         ''': "'",
#         "'": "'"
#     }
#     for old, new in replacements.items():
#         text = text.replace(old, new)
#     return text
#
#
# def is_punctuation(text):
#     """Check if the text is just punctuation"""
#     return all(not c.isalnum() for c in text)
#
#
# def parse_ner_file(path):
#     """Parse NER data from file with optimized processing"""
#     ner_words = []
#     ner_dict = {}  # For quick lookup
#
#     with Path(path).open(encoding="utf-8") as f:
#         for line in f:
#             if not line.strip():
#                 continue
#
#             # Split and normalize in one step
#             parts = [normalize_quotes(part.strip()) for part in line.split("\t") if part.strip()]
#
#             # Ensure we have at least two elements (word and tag)
#             if len(parts) >= 2:
#                 # Take only the first two elements (word and tag)
#                 word_info = parts[:2]
#                 ner_words.append(word_info)
#
#                 # Add to dictionary for direct lookup
#                 ner_dict[word_info[0]] = word_info
#
#     return ner_words, ner_dict
#
#
# def process_conllu_file(file_path):
#     """Process a single CONLLU file and return POS data"""
#     try:
#         with Path(file_path).open(encoding="utf-8") as f:
#             data = f.read()
#
#         pos_words = []
#         sentences = parse(data)
#         for sentence in sentences:
#             for token in sentence:
#                 word = normalize_quotes(token['form'])
#                 pos_tokens = [
#                     word,
#                     normalize_quotes(token['lemma']),
#                     token['upostag'],
#                     token['feats']
#                 ]
#                 pos_words.append(pos_tokens)
#
#         return pos_words
#     except Exception as e:
#         print(f"Error processing {file_path}: {e}")
#         return []
#
#
# def process_conllu_files_parallel(conllu_dir, max_files=1245):
#     """Process CONLLU files in parallel for better performance"""
#     file_paths = [conllu_dir / f"{i}.conllu" for i in range(1, max_files + 1)]
#     existing_paths = [p for p in file_paths if p.exists()]
#
#     # Use multiprocessing to speed up file processing
#     with mp.Pool(processes=mp.cpu_count()) as pool:
#         results = list(tqdm(
#             pool.imap(process_conllu_file, existing_paths),
#             total=len(existing_paths),
#             desc="Processing CONLLU files"
#         ))
#
#     # Flatten the list of lists
#     pos_words = [item for sublist in results for item in sublist]
#
#     # Create lookup dictionaries
#     pos_dict = {}
#     for entry in pos_words:
#         pos_dict[entry[0]] = entry
#
#     # Special dictionary for quotes
#     quotes_dict = {
#         '"': next((e for e in pos_words if e[0] in ['"', '"', '"']), None),
#         "'": next((e for e in pos_words if e[0] in ["'", ''', ''']), None)
#     }
#
#     return pos_words, pos_dict, quotes_dict
#
#
# def match_ner_with_pos(ner_words, pos_dict, quotes_dict, threshold=80):
#     """Match NER words with POS words efficiently"""
#     combined_words = []
#     unmatched_ner = []
#
#     # For fuzzy matching, prepare a list of words once
#     pos_word_list = list(pos_dict.keys())
#
#     for ner_entry in tqdm(ner_words, desc="Matching NER with POS"):
#         ner_word = ner_entry[0]
#         matched = False
#
#         # Skip empty words
#         if not ner_word.strip():
#             continue
#
#         # Direct match (most efficient)
#         if ner_word in pos_dict:
#             combined_words.append({'ner': ner_entry, 'pos': pos_dict[ner_word]})
#             matched = True
#
#         # Special handling for quotes
#         elif ner_word in ['"', '"', '"'] and quotes_dict['"']:
#             combined_words.append({'ner': ner_entry, 'pos': quotes_dict['"'], 'note': 'quote-match'})
#             matched = True
#
#         elif ner_word in ["'", ''', '''] and quotes_dict["'"]:
#             combined_words.append({'ner': ner_entry, 'pos': quotes_dict["'"], 'note': 'quote-match'})
#             matched = True
#
#         else:
#             # Only do fuzzy matching for non-punctuation
#             if not is_punctuation(ner_word):
#                 try:
#                     # Use rapidfuzz instead of fuzzywuzzy (much faster)
#                     matches = rapidfuzz.process.extractOne(
#                         ner_word,
#                         pos_word_list,
#                         score_cutoff=threshold
#                     )
#
#                     if matches:
#                         # Rapidfuzz returns a tuple of (match, score, index)
#                         best_match, score, _ = matches
#                         combined_words.append({
#                             'ner': ner_entry,
#                             'pos': pos_dict[best_match],
#                             'match_score': score
#                         })
#                         matched = True
#                 except Exception as e:
#                     print(f"Error matching '{ner_word}': {e}")
#
#         if not matched:
#             unmatched_ner.append(ner_entry)
#
#     return combined_words, unmatched_ner
#
#
# def main():
#     # Load NER data
#     print("Loading NER data...")
#     ner_words, ner_dict = parse_ner_file("korpusi.txt")
#
#     # Load POS data using parallel processing
#     print("Loading POS data...")
#     conllu_dir = Path("RightOrderConllu/")
#     pos_words, pos_dict, quotes_dict = process_conllu_files_parallel(conllu_dir)
#
#     print(f"NER: {len(ner_words)}")
#     print(f"POS: {len(pos_words)}")
#
#     # Match NER with POS
#     print("Matching NER with POS...")
#     combined_words, unmatched_ner = match_ner_with_pos(ner_words, pos_dict, quotes_dict)
#
#     # Write results to files
#     output_file = "combined_words.txt"
#     with open(output_file, "w", encoding="utf-8") as f:
#         for entry in combined_words:
#             f.write(f"{entry}\n")
#
#     unmatched_file = "unmatched_ner.txt"
#     with open(unmatched_file, "w", encoding="utf-8") as f:
#         for entry in unmatched_ner:
#             f.write(f"{entry}\n")
#
#     # Count matched vs unmatched quotation marks
#     quotes_matched = sum(1 for entry in combined_words if 'note' in entry and entry['note'] == 'quote-match')
#     quotes_unmatched = sum(1 for entry in unmatched_ner if entry[0] in ['"', '"', '"', "'", ''', '''])
#
#     print(f"\nCombined words written to {output_file}")
#     print(f"Unmatched NER words written to {unmatched_file}")
#     print(f"Successfully matched: {len(combined_words)} out of {len(ner_words)} NER words")
#     print(f"Match rate: {len(combined_words) / len(ner_words) * 100:.2f}%")
#     print(f"Quotation marks matched: {quotes_matched}, unmatched: {quotes_unmatched}")
#
#
# if __name__ == "__main__":
#     main()


from conllu import parse
from pathlib import Path
import logging
from collections import defaultdict
import multiprocessing as mp
from tqdm import tqdm  # For progress bar
import rapidfuzz.process  # Much faster than fuzzywuzzy

logging.getLogger().setLevel(logging.ERROR)


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


def parse_ner_file(path):
    """Parse NER file and return ordered list of (word, tag) tuples"""
    ner_list = []

    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            parts = [normalize_quotes(part.strip()) for part in line.split("\t") if part.strip()]

            if len(parts) >= 2:
                word = parts[0]
                ner_tag = parts[1]
                ner_list.append((word, ner_tag))

    return ner_list


def format_conllu_field(field):
    """Convert CoNLL-U field to proper string format"""
    if field is None:
        return "_"
    elif isinstance(field, dict):
        if not field:
            return "_"
        # Convert dict to key=value|key=value format
        parts = []
        for key, value in field.items():
            if value is not None:
                parts.append(f"{key}={value}")
        return "|".join(parts) if parts else "_"
    elif isinstance(field, str):
        return field if field else "_"
    else:
        return str(field) if field else "_"


def process_conllu_file(file_path):
    """Process a single CoNLL-U file and return sentences with their structure"""
    try:
        with Path(file_path).open(encoding="utf-8") as f:
            data = f.read()

        sentences_info = []
        sentences = parse(data)

        for sentence in sentences:
            # Extract sentence metadata
            sent_id = sentence.metadata.get('sent_id', 'unknown')
            text = sentence.metadata.get('text', '')

            sentence_tokens = []
            for token in sentence:
                # Store complete token information
                token_info = {
                    'id': token['id'],
                    'form': normalize_quotes(token['form']),
                    'lemma': normalize_quotes(token['lemma']),
                    'upostag': token['upostag'],
                    'xpostag': format_conllu_field(token.get('xpostag')),
                    'feats': format_conllu_field(token['feats']),
                    'head': format_conllu_field(token.get('head')),
                    'deprel': format_conllu_field(token.get('deprel')),
                    'deps': format_conllu_field(token.get('deps')),
                    'misc': format_conllu_field(token.get('misc'))
                }
                sentence_tokens.append(token_info)

            # Store sentence info with its tokens
            sentences_info.append({
                'sent_id': sent_id,
                'text': text,
                'tokens': sentence_tokens,
                'metadata': sentence.metadata
            })

        return sentences_info
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
    """Process all CoNLL-U files and return all sentences"""
    file_paths = get_all_conllu_files(conllu_dir)

    if not file_paths:
        print(f"Warning: No CONLLU files found in subdirectories of {conllu_dir}")
        return []

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = list(tqdm(
            pool.imap(process_conllu_file, file_paths),
            total=len(file_paths),
            desc="Processing CONLLU files"
        ))

    # Flatten all sentences from all files
    all_sentences = []
    for sentences_from_file in results:
        all_sentences.extend(sentences_from_file)

    return all_sentences


def extract_ner_for_sentence(sentence_tokens, ner_list, ner_counter):
    """
    Extract NER entries that correspond to the current sentence.
    Returns: (ner_entries_for_sentence, new_ner_counter)
    """
    sentence_words = [token['form'] for token in sentence_tokens]
    sentence_ner_entries = []
    current_counter = ner_counter

    print(f"Looking for sentence words: {sentence_words}")
    print(f"Starting from NER position: {current_counter}")

    # Try to find all words of the sentence in the NER list sequentially
    for word in sentence_words:
        found = False
        # Look ahead a bit in case of minor misalignments
        for look_ahead in range(5):  # Check current position and next 4 positions
            check_pos = current_counter + look_ahead
            if check_pos < len(ner_list):
                ner_word, ner_tag = ner_list[check_pos]
                print(f"Comparing '{word}' with NER[{check_pos}]: '{ner_word}'")

                if word == ner_word:
                    sentence_ner_entries.append((ner_word, ner_tag))
                    current_counter = check_pos + 1
                    print(f"Exact match found! Moving counter to {current_counter}")
                    found = True
                    break

        if not found:
            # Try fuzzy matching within a small window
            fuzzy_found = False
            for look_ahead in range(3):
                check_pos = current_counter + look_ahead
                if check_pos < len(ner_list):
                    ner_word, ner_tag = ner_list[check_pos]
                    if not is_punctuation(word) and not is_punctuation(ner_word):
                        try:
                            similarity = rapidfuzz.process.extractOne(
                                word, [ner_word], score_cutoff=85
                            )
                            if similarity:
                                sentence_ner_entries.append((ner_word, ner_tag))
                                current_counter = check_pos + 1
                                print(f"Fuzzy match found: '{word}' -> '{ner_word}' (score: {similarity[1]})")
                                fuzzy_found = True
                                break
                        except:
                            pass

            if not fuzzy_found:
                # Add placeholder for unmatched word
                sentence_ner_entries.append((word, "O"))
                print(f"No match found for '{word}', adding placeholder")

    return sentence_ner_entries, current_counter


def match_ner_to_pos_tokens(sentence_ner_entries, sentence_pos_tokens):
    """
    Match NER entries to POS tokens, prioritizing NER data.
    Returns list of matched tokens with NER information.
    """
    matched_tokens = []

    # Direct alignment first (same length)
    if len(sentence_ner_entries) == len(sentence_pos_tokens):
        for i, (pos_token, (ner_word, ner_tag)) in enumerate(zip(sentence_pos_tokens, sentence_ner_entries)):
            pos_token_copy = pos_token.copy()

            # Add NER information to MISC field
            misc_parts = []
            existing_misc = pos_token.get('misc', '_')
            if existing_misc and existing_misc != '_':
                misc_parts.append(str(existing_misc))

            if ner_tag != "O":
                misc_parts.append(f"NER={ner_tag}")
                if pos_token['form'] == ner_word:
                    misc_parts.append("MATCH=exact")
                else:
                    misc_parts.append("MATCH=aligned")

            pos_token_copy['misc'] = "|".join(misc_parts) if misc_parts else "_"
            matched_tokens.append(pos_token_copy)

    else:
        # Handle length mismatch - use fuzzy matching
        ner_words = [entry[0] for entry in sentence_ner_entries]
        ner_tags_dict = {entry[0]: entry[1] for entry in sentence_ner_entries}

        for pos_token in sentence_pos_tokens:
            pos_token_copy = pos_token.copy()
            pos_word = pos_token['form']

            # Find best NER match for this POS token
            best_ner_word = None
            best_ner_tag = "O"
            match_type = "none"

            # Try exact match first
            if pos_word in ner_tags_dict:
                best_ner_word = pos_word
                best_ner_tag = ner_tags_dict[pos_word]
                match_type = "exact"
            else:
                # Try fuzzy matching with NER words
                if not is_punctuation(pos_word):
                    try:
                        matches = rapidfuzz.process.extractOne(
                            pos_word, ner_words, score_cutoff=75
                        )
                        if matches:
                            best_ner_word = matches[0]
                            best_ner_tag = ner_tags_dict[best_ner_word]
                            match_type = f"fuzzy_{matches[1]:.1f}"
                    except:
                        pass

            # Add NER information to MISC field
            misc_parts = []
            existing_misc = pos_token.get('misc', '_')
            if existing_misc and existing_misc != '_':
                misc_parts.append(str(existing_misc))

            if best_ner_tag != "O":
                misc_parts.append(f"NER={best_ner_tag}")
                misc_parts.append(f"MATCH={match_type}")

            pos_token_copy['misc'] = "|".join(misc_parts) if misc_parts else "_"
            matched_tokens.append(pos_token_copy)

    return matched_tokens


def process_sentences_with_ner_sequence(all_sentences, ner_list,
                                        output_file="combined_words.conllu",
                                        stats_file="ner_matching_stats.txt"):
    """
    Process sentences by extracting sequential NER entries and matching with POS tokens
    """

    stats = {
        'total_sentences': len(all_sentences),
        'total_tokens': 0,
        'sentences_processed': 0,
        'tokens_with_ner': 0,
        'exact_matches': 0,
        'fuzzy_matches': 0,
        'no_matches': 0,
        'ner_entries_used': 0
    }

    ner_counter = 0  # Track position in NER list

    with open(output_file, "w", encoding="utf-8") as f_combined:
        # Write CoNLL-U header
        f_combined.write("# newdoc\n")

        for sentence_info in tqdm(all_sentences, desc="Processing sentences with NER sequence"):
            if ner_counter >= len(ner_list):
                print(f"Reached end of NER list at sentence {stats['sentences_processed']}")
                break

            sent_id = sentence_info['sent_id']
            text = sentence_info['text']
            tokens = sentence_info['tokens']
            metadata = sentence_info['metadata']

            print(f"\nProcessing sentence {sent_id}: {text}")

            # Extract NER entries for this sentence
            sentence_ner_entries, new_ner_counter = extract_ner_for_sentence(
                tokens, ner_list, ner_counter
            )

            print(f"Extracted {len(sentence_ner_entries)} NER entries")
            print(f"NER counter moved from {ner_counter} to {new_ner_counter}")

            # Match NER entries with POS tokens
            matched_tokens = match_ner_to_pos_tokens(sentence_ner_entries, tokens)

            # Write sentence metadata
            f_combined.write(f"# sent_id = {sent_id}\n")
            f_combined.write(f"# text = {text}\n")

            # Write any additional metadata
            for key, value in metadata.items():
                if key not in ['sent_id', 'text']:
                    f_combined.write(f"# {key} = {value}\n")

            # Write tokens
            for token in matched_tokens:
                stats['total_tokens'] += 1

                # Count matches
                misc_field = str(token.get('misc', '_'))
                if 'NER=' in misc_field:
                    stats['tokens_with_ner'] += 1
                    if 'MATCH=exact' in misc_field:
                        stats['exact_matches'] += 1
                    elif 'MATCH=fuzzy' in misc_field or 'fuzzy' in misc_field:
                        stats['fuzzy_matches'] += 1
                else:
                    stats['no_matches'] += 1

                # Write token in CoNLL-U format
                conllu_line = (f"{token['id']}\t{token['form']}\t{token['lemma']}\t"
                               f"{token['upostag']}\t{token['xpostag']}\t{token['feats']}\t"
                               f"{token['head']}\t{token['deprel']}\t{token['deps']}\t{token['misc']}")

                f_combined.write(f"{conllu_line}\n")

            # Empty line to separate sentences
            f_combined.write("\n")

            # Update counters
            ner_counter = new_ner_counter
            stats['sentences_processed'] += 1
            stats['ner_entries_used'] = ner_counter

    # Write statistics
    with open(stats_file, "w", encoding="utf-8") as f_stats:
        f_stats.write("NER Sequential Matching Statistics\n")
        f_stats.write("==================================\n\n")
        f_stats.write(f"Total sentences available: {stats['total_sentences']}\n")
        f_stats.write(f"Sentences processed: {stats['sentences_processed']}\n")
        f_stats.write(f"Total NER entries: {len(ner_list)}\n")
        f_stats.write(f"NER entries used: {stats['ner_entries_used']}\n")
        f_stats.write(f"NER utilization: {stats['ner_entries_used'] / len(ner_list) * 100:.2f}%\n\n")
        f_stats.write(f"Total tokens processed: {stats['total_tokens']}\n")
        f_stats.write(f"Tokens with NER tags: {stats['tokens_with_ner']}\n")
        f_stats.write(f"Exact matches: {stats['exact_matches']}\n")
        f_stats.write(f"Fuzzy matches: {stats['fuzzy_matches']}\n")
        f_stats.write(f"No matches: {stats['no_matches']}\n\n")
        f_stats.write(f"NER coverage: {stats['tokens_with_ner'] / stats['total_tokens'] * 100:.2f}%\n")
        f_stats.write(f"Exact match rate: {stats['exact_matches'] / stats['total_tokens'] * 100:.2f}%\n")
        f_stats.write(f"Fuzzy match rate: {stats['fuzzy_matches'] / stats['total_tokens'] * 100:.2f}%\n")

    return stats


def main():
    print("Loading NER data...")
    ner_list = parse_ner_file("../../Corpus/korpusi.txt")
    print(f"Loaded {len(ner_list)} NER entries")

    print("Loading POS sentences...")
    conllu_dir = Path("../../RightOrderConllu/")
    all_sentences = process_conllu_files_parallel(conllu_dir)
    print(f"Loaded {len(all_sentences)} sentences")

    if not all_sentences:
        print("Error: No sentences were loaded. Please check your directory structure.")
        return

    if not ner_list:
        print("Error: No NER data was loaded. Please check your NER file.")
        return

    print("Processing sentences with sequential NER matching...")
    output_file = "../../combined_words.conllu"
    stats_file = "../../ner_matching_stats.txt"

    stats = process_sentences_with_ner_sequence(
        all_sentences,
        ner_list,
        output_file=output_file,
        stats_file=stats_file
    )

    print(f"\nProcessing complete!")
    print(f"Output written to: {output_file}")
    print(f"Statistics written to: {stats_file}")
    print(f"\nQuick Stats:")
    print(f"- Processed {stats['sentences_processed']} sentences")
    print(f"- Used {stats['ner_entries_used']} out of {len(ner_list)} NER entries")
    print(
        f"- {stats['tokens_with_ner']} tokens with NER tags ({stats['tokens_with_ner'] / stats['total_tokens'] * 100:.2f}%)")
    print(f"- {stats['exact_matches']} exact matches, {stats['fuzzy_matches']} fuzzy matches")


if __name__ == "__main__":
    main()

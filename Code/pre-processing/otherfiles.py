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
#     replacements = {
#         '"': '"',
#         '“': '"',
#         '”': '"',
#         '‘': "'",
#         '’': "'",
#         "'": "'"
#     }
#     for old, new in replacements.items():
#         text = text.replace(old, new)
#     return text
#
#
# def is_punctuation(text):
#     return all(not c.isalnum() for c in text)
#
#
# def parse_ner_file(path):
#     ner_words = []
#     ner_dict = {}
#
#     with Path(path).open(encoding="utf-8") as f:
#         for line in f:
#             if not line.strip():
#                 continue
#
#             parts = [normalize_quotes(part.strip()) for part in line.split("\t") if part.strip()]
#
#             if len(parts) >= 2:
#                 word_info = parts[:2]
#                 ner_words.append(word_info)
#
#                 ner_dict[word_info[0]] = word_info
#
#     return ner_words, ner_dict
#
#
# def process_conllu_file(file_path):
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
# def get_all_conllu_files(conllu_dir):
#     conllu_files = []
#
#     for i in range(1, 10):
#         subdir = conllu_dir / f"{i}Part"
#         if subdir.exists() and subdir.is_dir():
#             subdir_files = list(subdir.glob("*.conllu"))
#             conllu_files.extend(subdir_files)
#             print(f"Found {len(subdir_files)} CONLLU files in {subdir}")
#
#     return conllu_files
#
#
# def process_conllu_files_parallel(conllu_dir):
#
#     file_paths = get_all_conllu_files(conllu_dir)
#
#     if not file_paths:
#         print(f"Warning: No CONLLU files found in subdirectories of {conllu_dir}")
#         return [], {}, {}
#
#     with mp.Pool(processes=mp.cpu_count()) as pool:
#         results = list(tqdm(
#             pool.imap(process_conllu_file, file_paths),
#             total=len(file_paths),
#             desc="Processing CONLLU files"
#         ))
#
#     pos_words = [item for sublist in results for item in sublist]
#
#     pos_dict = {}
#     for entry in pos_words:
#         pos_dict[entry[0]] = entry
#
#     quotes_dict = {
#         '"': next((e for e in pos_words if e[0] in ['"', '“', '”']), None),
#         "'": next((e for e in pos_words if e[0] in ["'", '‘', '’']), None)
#     }
#
#     return pos_words, pos_dict, quotes_dict
#
#
# def match_ner_with_pos(ner_words, pos_dict, quotes_dict, threshold=80,
#                        output_file="combined_words.conllu", unmatched_file="unmatched_ner.txt"):
#     """Match NER words with POS words efficiently and write results in CoNLL-U format"""
#     # Open files for writing
#     with open(output_file, "w", encoding="utf-8") as f_combined,open(unmatched_file, "w", encoding="utf-8") as f_unmatched:
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
#             # Start a new sentence every 100 tokens (can be adjusted)
#             if token_id > 100:
#                 current_sentence_id += 1
#                 token_id = 1
#                 f_combined.write("\n# sent_id = {}\n".format(current_sentence_id))
#                 f_combined.write("# text = Generated combined NER and POS data (continued)\n")
#
#             # Direct match (most efficient)
#             if ner_word in pos_dict:
#                 pos_info = pos_dict[ner_word]
#                 # Format in CoNLL-U:
#                 # ID FORM LEMMA UPOS XPOS FEATS HEAD DEPREL DEPS MISC
#                 lemma = pos_info[1] if len(pos_info) > 1 else "_"
#                 upos = pos_info[2] if len(pos_info) > 2 else "_"
#                 feats = pos_info[3] if len(pos_info) > 3 else "_"
#
#                 # Write in CoNLL-U format
#                 conllu_line = f"{token_id}\t{ner_word}\t{lemma}\t{upos}\t_\t{feats}\t_\t_\t_\tNER={ner_tag}|MATCH=exact"
#                 f_combined.write(f"{conllu_line}\n")
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
#
#                             conllu_line = f"{token_id}\t{ner_word}\t{lemma}\t{upos}\t_\t{feats}\t_\t_\t_\tNER={ner_tag}|MATCH=fuzzy|SCORE={score:.1f}"
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
#
#
# def main():
#
#     print("Loading NER data...")
#     ner_words, ner_dict = parse_ner_file("korpusi.txt")
#
#     print("Loading POS data...")
#     conllu_dir = Path("RightOrderConllu/")
#     pos_words, pos_dict, quotes_dict = process_conllu_files_parallel(conllu_dir)
#
#     print(f"NER: {len(ner_words)}")
#     print(f"POS: {len(pos_words)}")
#
#     if not pos_words:
#         print("Error: No POS data was loaded. Please check your directory structure.")
#         return
#
#     print("Matching NER with POS and writing to CoNLL-U file...")
#     output_file = "combined_words.conllu"
#     unmatched_file = "unmatched_ner.txt"
#
#     stats = match_ner_with_pos(
#         ner_words,
#         pos_dict,
#         quotes_dict,
#         output_file=output_file,
#         unmatched_file=unmatched_file
#     )
#
#     print(f"\nCombined words written to {output_file} in CoNLL-U format")
#     print(f"Unmatched NER words written to {unmatched_file}")
#     print(f"Successfully matched: {stats['matched_count']} out of {stats['total_ner_words']} NER words")
#     print(f"Match rate: {stats['matched_count'] / stats['total_ner_words'] * 100:.2f}%")
#     print(f"Quotation marks matched: {stats['quotes_matched']}, unmatched: {stats['quotes_unmatched']}")
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
        sentences_info = []  # Store sentence metadata
        sentences = parse(data)

        for sentence in sentences:
            # Extract sentence metadata
            sent_id = sentence.metadata.get('sent_id', 'unknown')
            text = sentence.metadata.get('text', '')

            sentence_tokens = []
            for token in sentence:
                word = normalize_quotes(token['form'])
                pos_tokens = [
                    word,
                    normalize_quotes(token['lemma']),
                    token['upostag'],
                    token['feats']
                ]
                pos_words.append(pos_tokens)
                sentence_tokens.append(pos_tokens)

            # Store sentence info with its tokens
            sentences_info.append({
                'sent_id': sent_id,
                'text': text,
                'tokens': sentence_tokens,
                'metadata': sentence.metadata
            })

        return pos_words, sentences_info
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return [], []


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
        return [], {}, {}, []

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = list(tqdm(
            pool.imap(process_conllu_file, file_paths),
            total=len(file_paths),
            desc="Processing CONLLU files"
        ))

    # Flatten pos_words and collect all sentences
    pos_words = []
    all_sentences = []

    for pos_words_file, sentences_file in results:
        pos_words.extend(pos_words_file)
        all_sentences.extend(sentences_file)

    pos_dict = {}
    for entry in pos_words:
        pos_dict[entry[0]] = entry

    quotes_dict = {
        '"': next((e for e in pos_words if e[0] in ['"', '“', '”']), None),
        "'": next((e for e in pos_words if e[0] in ["'", '‘', '’']), None)
    }

    return pos_words, pos_dict, quotes_dict, all_sentences


def match_ner_with_pos_preserve_sentences(ner_words, pos_dict, quotes_dict, all_sentences, threshold=80,
                                          output_file="combined_words.conllu", unmatched_file="unmatched_ner.txt"):
    """Match NER words with POS words and preserve original sentence structure"""

    # Create a mapping of words to their sentence context
    word_to_sentences = defaultdict(list)
    for sent_idx, sentence_info in enumerate(all_sentences):
        for token in sentence_info['tokens']:
            word = token[0]
            word_to_sentences[word].append((sent_idx, sentence_info))

    with open(output_file, "w", encoding="utf-8") as f_combined, open(unmatched_file, "w",
                                                                      encoding="utf-8") as f_unmatched:

        matched_count = 0
        ner_word_index = 0

        # Write CoNLL-U header
        f_combined.write("# newdoc\n")

        # Group NER words by sentences when possible
        current_sentence_idx = 0
        written_sentences = set()

        for ner_entry in tqdm(ner_words, desc="Matching NER with POS"):
            ner_word = ner_entry[0]
            ner_tag = ner_entry[1] if len(ner_entry) > 1 else "_"
            matched = False

            # Skip empty words
            if not ner_word.strip():
                continue

            # Try to find the sentence context for this word
            sentence_context = None
            if ner_word in word_to_sentences:
                # Use the first available sentence context
                sentence_context = word_to_sentences[ner_word][0][1]

            # If we have a sentence context and haven't written this sentence yet
            if sentence_context and sentence_context['sent_id'] not in written_sentences:
                # Write the complete sentence
                f_combined.write(f"# sent_id = {sentence_context['sent_id']}\n")
                f_combined.write(f"# text = {sentence_context['text']}\n")

                # Write any additional metadata
                for key, value in sentence_context['metadata'].items():
                    if key not in ['sent_id', 'text']:
                        f_combined.write(f"# {key} = {value}\n")

                # Match and write all tokens in this sentence
                token_id = 1
                for token in sentence_context['tokens']:
                    word = token[0]

                    # Check if this word is in our NER data
                    ner_match = None
                    for ner_item in ner_words:
                        if ner_item[0] == word:
                            ner_match = ner_item[1]
                            break

                    # Write token in CoNLL-U format
                    lemma = token[1] if len(token) > 1 else "_"
                    upos = token[2] if len(token) > 2 else "_"
                    feats = token[3] if len(token) > 3 else "_"

                    misc_field = "MATCH=exact"
                    if ner_match:
                        misc_field += f"|NER={ner_match}"

                    conllu_line = f"{token_id}\t{word}\t{lemma}\t{upos}\t_\t{feats}\t_\t_\t_\t{misc_field}"
                    f_combined.write(f"{conllu_line}\n")
                    token_id += 1

                f_combined.write("\n")  # Empty line to separate sentences
                written_sentences.add(sentence_context['sent_id'])
                matched = True
                matched_count += 1

            # If no sentence context found, try individual word matching
            elif ner_word in pos_dict:
                # Handle individual words without sentence context
                if current_sentence_idx not in written_sentences:
                    f_combined.write(f"# sent_id = individual_{current_sentence_idx}\n")
                    f_combined.write(f"# text = Individual word matches\n")
                    written_sentences.add(current_sentence_idx)

                pos_info = pos_dict[ner_word]
                lemma = pos_info[1] if len(pos_info) > 1 else "_"
                upos = pos_info[2] if len(pos_info) > 2 else "_"
                feats = pos_info[3] if len(pos_info) > 3 else "_"

                conllu_line = f"1\t{ner_word}\t{lemma}\t{upos}\t_\t{feats}\t_\t_\t_\tNER={ner_tag}|MATCH=exact"
                f_combined.write(f"{conllu_line}\n\n")
                matched = True
                matched_count += 1
                current_sentence_idx += 1

            else:
                # Try fuzzy matching for individual words
                if not is_punctuation(ner_word):
                    try:
                        pos_word_list = list(pos_dict.keys())
                        matches = rapidfuzz.process.extractOne(
                            ner_word,
                            pos_word_list,
                            score_cutoff=threshold
                        )

                        if matches:
                            best_match, score, _ = matches
                            pos_info = pos_dict[best_match]

                            if current_sentence_idx not in written_sentences:
                                f_combined.write(f"# sent_id = fuzzy_{current_sentence_idx}\n")
                                f_combined.write(f"# text = Fuzzy matched words\n")
                                written_sentences.add(current_sentence_idx)

                            lemma = pos_info[1] if len(pos_info) > 1 else "_"
                            upos = pos_info[2] if len(pos_info) > 2 else "_"
                            feats = pos_info[3] if len(pos_info) > 3 else "_"

                            conllu_line = f"1\t{ner_word}\t{lemma}\t{upos}\t_\t{feats}\t_\t_\t_\tNER={ner_tag}|MATCH=fuzzy|SCORE={score:.1f}"
                            f_combined.write(f"{conllu_line}\n\n")
                            matched = True
                            matched_count += 1
                            current_sentence_idx += 1
                    except Exception as e:
                        print(f"Error matching '{ner_word}': {e}")

            if not matched:
                f_unmatched.write(f"{ner_word}\t{ner_tag}\n")

    return {
        'matched_count': matched_count,
        'unmatched_count': len(ner_words) - matched_count,
        'total_ner_words': len(ner_words),
        'sentences_preserved': len(written_sentences)
    }


def main():
    print("Loading NER data...")
    ner_words, ner_dict = parse_ner_file("../../Corpus/korpusi.txt")

    print("Loading POS data...")
    conllu_dir = Path("../../RightOrderConllu/")
    pos_words, pos_dict, quotes_dict, all_sentences = process_conllu_files_parallel(conllu_dir)

    print(f"NER: {len(ner_words)}")
    print(f"POS: {len(pos_words)}")
    print(f"Sentences found: {len(all_sentences)}")

    if not pos_words:
        print("Error: No POS data was loaded. Please check your directory structure.")
        return

    print("Matching NER with POS and preserving original sentences...")
    output_file = "../../combined_words.conllu"
    unmatched_file = "../../unmatched_ner.txt"

    stats = match_ner_with_pos_preserve_sentences(
        ner_words,
        pos_dict,
        quotes_dict,
        all_sentences,
        output_file=output_file,
        unmatched_file=unmatched_file
    )

    print(f"\nCombined words written to {output_file} in CoNLL-U format")
    print(f"Unmatched NER words written to {unmatched_file}")
    print(f"Successfully matched: {stats['matched_count']} out of {stats['total_ner_words']} NER words")
    print(f"Match rate: {stats['matched_count'] / stats['total_ner_words'] * 100:.2f}%")
    print(f"Sentences preserved: {stats['sentences_preserved']}")


if __name__ == "__main__":
    main()
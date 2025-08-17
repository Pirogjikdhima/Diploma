import logging
import multiprocessing as mp
from pathlib import Path

import rapidfuzz.process
from conllu import parse
from tqdm import tqdm

from functions.functions import *

logging.getLogger().setLevel(logging.ERROR)


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
                    token['feats']
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


def match_ner_with_pos(ner_words, pos_dict, quotes_dict, threshold=80,
                       output_file="combined_words.conllu", unmatched_file="unmatched_ner.txt"):
    """Match NER words with POS words efficiently and write results in CoNLL-U format"""
    # Open files for writing
    with open(output_file, "w", encoding="utf-8") as f_combined, open(unmatched_file, "w",
                                                                      encoding="utf-8") as f_unmatched:

        # For stats tracking
        matched_count = 0
        quotes_matched = 0
        quotes_unmatched = 0
        current_sentence_id = 1

        # Write CoNLL-U header
        f_combined.write("# newdoc\n")
        f_combined.write(f"# sent_id = {current_sentence_id}\n")
        f_combined.write("# text = Generated combined NER and POS data\n")

        # For fuzzy matching, prepare a list of words once
        pos_word_list = list(pos_dict.keys())
        token_id = 1

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

            # Direct match (most efficient)
            elif ner_word in pos_dict:
                pos_info = pos_dict[ner_word]
                # Format: WORD    NER_TAG    LEMMA    UPOS    FEATS
                lemma = pos_info[1] if len(pos_info) > 1 and pos_info[1] else "_"
                upos = pos_info[2] if len(pos_info) > 2 and pos_info[2] else "_"
                feats = pos_info[3] if len(pos_info) > 3 and pos_info[3] else "_"

                # Write in simple tab-separated format
                line = f"{ner_word}\t\t{ner_tag}\t{lemma}\t{upos}\t{feats}"
                f_combined.write(f"{line}\n")
                matched = True

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
                            pos_info = pos_dict[best_match]
                            lemma = pos_info[1] if len(pos_info) > 1 else "_"
                            upos = pos_info[2] if len(pos_info) > 2 else "_"
                            feats = pos_info[3] if len(pos_info) > 3 else "_"

                            conllu_line = f"{token_id}\t{ner_word}\t{lemma}\t{upos}\t_\t{feats}\t_\t_\t_\tNER={ner_tag}|MATCH=fuzzy|SCORE={score:.1f}"
                            f_combined.write(f"{conllu_line}\n")
                            matched = True
                    except Exception as e:
                        print(f"Error matching '{ner_word}': {e}")

            if not matched:
                f_unmatched.write(f"{ner_word}\t{ner_tag}\n")
                if ner_word in ['"', '“', '”', "'", '‘', '’']:
                    quotes_unmatched += 1
            else:
                matched_count += 1
                token_id += 1

    return {
        'matched_count': matched_count,
        'unmatched_count': len(ner_words) - matched_count,
        'quotes_matched': quotes_matched,
        'quotes_unmatched': quotes_unmatched,
        'total_ner_words': len(ner_words)
    }


def main():
    print("Loading NER data...")
    ner_words, ner_dict = parse_ner_file("../../Corpus/korpusi.txt")

    print("Loading POS data...")
    conllu_dir = Path("../../Conllu Files in Corpus/")
    pos_words, pos_dict, quotes_dict = process_conllu_files_parallel(conllu_dir)

    print(f"NER: {len(ner_words)}")
    print(f"POS: {len(pos_words)}")

    if not pos_words:
        print("Error: No POS data was loaded. Please check your directory structure.")
        return

    print("Matching NER with POS and writing to CoNLL-U file...")
    output_file = "../../Dataset/Testing/text_dataset.txt"
    unmatched_file = "../../Dataset/Errors/unmatched_ner.txt"

    stats = match_ner_with_pos(
        ner_words,
        pos_dict,
        quotes_dict,
        output_file=output_file,
        unmatched_file=unmatched_file
    )

    print(f"\nCombined words written to {output_file} in CoNLL-U format")
    print(f"Unmatched NER words written to {unmatched_file}")
    print(f"Successfully matched: {stats['matched_count']} out of {stats['total_ner_words']} NER words")
    print(f"Match rate: {stats['matched_count'] / stats['total_ner_words'] * 100:.2f}%")
    print(f"Quotation marks matched: {stats['quotes_matched']}, unmatched: {stats['quotes_unmatched']}")


if __name__ == "__main__":
    main()

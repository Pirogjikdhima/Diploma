from functions.functions import *
import logging
logging.getLogger().setLevel(logging.ERROR)


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

    print("Matching NER with POS and writing to file...")
    output_file = "../../Dataset/Testing/combined_words.csv"
    unmatched_file = "../../Dataset/Errors/unmatched_ner.txt"

    # stats = match_ner_with_pos(
    #     ner_words,
    #     pos_dict,
    #     quotes_dict,
    #     output_file=output_file,
    #     unmatched_file=unmatched_file
    # )
    stats = match_ner_with_pos_sequential_csv(
        ner_words,
        pos_words,
        output_file=output_file,
        unmatched_file=unmatched_file
    )

    print(f"\nCombined words written to {output_file} in CSV format")
    print(f"Unmatched NER words written to {unmatched_file}")
    print(f"Successfully matched: {stats['matched_count']} out of {stats['total_ner_words']} NER words")
    print(f"Match rate: {stats['matched_count'] / stats['total_ner_words'] * 100:.2f}%")
    print(f"Unmatched NER words written to {stats['word_usage_stats']}")
    print(f"Quotation marks matched: {stats['quotes_matched']}, unmatched: {stats['quotes_unmatched']}")



if __name__ == "__main__":
    main()

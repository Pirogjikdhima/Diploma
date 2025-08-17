from conllu import parse
from pathlib import Path
import multiprocessing as mp
from tqdm import tqdm
import logging

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



def process_conllu_file(file_path):
    """Process a single CoNLL-U file and extract sentences"""
    try:
        with Path(file_path).open(encoding="utf-8") as f:
            data = f.read()

        sentences_text = []
        sentences = parse(data)

        for sentence in sentences:
            # Try to get the text from metadata first
            if 'text' in sentence.metadata and sentence.metadata['text'].strip():
                sentence_text = normalize_quotes(sentence.metadata['text'].strip())
            else:
                # If no text metadata, reconstruct from tokens
                words = []
                for token in sentence:
                    if isinstance(token['id'], int):  # Skip multiword tokens
                        words.append(normalize_quotes(token['form']))
                sentence_text = " ".join(words)

            if sentence_text:  # Only add non-empty sentences
                sentences_text.append(sentence_text)

        return sentences_text
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []


def get_all_conllu_files(conllu_dir):
    """Find all .conllu files in the directory structure"""
    conllu_files = []

    # Check for numbered subdirectories (1Part, 2Part, etc.)
    for i in range(1, 10):
        subdir = conllu_dir / f"{i}Part"
        if subdir.exists() and subdir.is_dir():
            subdir_files = list(subdir.glob("*.conllu"))
            conllu_files.extend(subdir_files)
            print(f"Found {len(subdir_files)} CONLLU files in {subdir}")

    # Also check the main directory
    main_files = list(conllu_dir.glob("*.conllu"))
    if main_files:
        conllu_files.extend(main_files)
        print(f"Found {len(main_files)} CONLLU files in main directory")

    return conllu_files


def extract_sentences_from_conllu(conllu_dir, output_file):
    """
    Extract all sentences from CoNLL-U files and save to text file.
    Each sentence is saved on a new line.
    """
    print("Finding CoNLL-U files...")
    file_paths = get_all_conllu_files(Path(conllu_dir))

    if not file_paths:
        print(f"Warning: No CONLLU files found in {conllu_dir}")
        return []

    print(f"Processing {len(file_paths)} CoNLL-U files...")

    # Process files in parallel
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

    print(f"Extracted {len(all_sentences)} sentences")

    # Write sentences to output file (one per line)
    print(f"Writing sentences to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as outfile:
        for sentence in all_sentences:
            outfile.write(sentence + "\n")

    print(f"Successfully saved {len(all_sentences)} sentences to {output_file}")
    return all_sentences


def extract_sentences_with_metadata(conllu_dir, output_file, metadata_file=None):
    """
    Extract sentences with additional metadata information.
    Optionally save metadata to a separate file.
    """
    print("Finding CoNLL-U files...")
    file_paths = get_all_conllu_files(Path(conllu_dir))

    if not file_paths:
        print(f"Warning: No CONLLU files found in {conllu_dir}")
        return []

    print(f"Processing {len(file_paths)} CoNLL-U files...")

    all_sentences = []
    sentence_metadata = []

    for file_path in tqdm(file_paths, desc="Processing CONLLU files"):
        try:
            with Path(file_path).open(encoding="utf-8") as f:
                data = f.read()

            sentences = parse(data)

            for sentence in sentences:
                # Extract sentence text
                if 'text' in sentence.metadata and sentence.metadata['text'].strip():
                    sentence_text = normalize_quotes(sentence.metadata['text'].strip())
                else:
                    words = []
                    for token in sentence:
                        if isinstance(token['id'], int):
                            words.append(normalize_quotes(token['form']))
                    sentence_text = " ".join(words)

                if sentence_text:
                    all_sentences.append(sentence_text)

                    # Store metadata
                    meta_info = {
                        'file': str(file_path),
                        'sent_id': sentence.metadata.get('sent_id', 'unknown'),
                        'length': len(sentence_text.split()),
                        'tokens': len([t for t in sentence if isinstance(t['id'], int)])
                    }
                    sentence_metadata.append(meta_info)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # Write sentences to output file
    print(f"Writing {len(all_sentences)} sentences to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as outfile:
        for sentence in all_sentences:
            outfile.write(sentence + "\n")

    # Write metadata if requested
    if metadata_file:
        print(f"Writing metadata to {metadata_file}...")
        with open(metadata_file, "w", encoding="utf-8") as metafile:
            metafile.write("sentence_index\tfile\tsent_id\tword_count\ttoken_count\n")
            for i, meta in enumerate(sentence_metadata):
                metafile.write(f"{i + 1}\t{meta['file']}\t{meta['sent_id']}\t{meta['length']}\t{meta['tokens']}\n")

    print(f"Successfully processed {len(all_sentences)} sentences")
    return all_sentences


def main():
    """Main function to extract sentences from CoNLL-U files"""
    # Configuration
    conllu_directory = "../../Conllu Files in Corpus/"
    output_filename = "extracted_sentences.txt"
    metadata_filename = "sentence_metadata.txt"

    print("CoNLL-U Sentence Extractor")
    print("=" * 50)

    # Extract sentences (basic version)
    sentences = extract_sentences_from_conllu(conllu_directory, output_filename)

    if sentences:
        print(f"\nExtraction completed successfully!")
        print(f"Output file: {output_filename}")
        print(f"Total sentences: {len(sentences)}")

        # Show first few sentences as preview
        print(f"\nFirst 3 sentences preview:")
        for i, sentence in enumerate(sentences[:3]):
            print(f"{i + 1}: {sentence}")

        if len(sentences) > 3:
            print("...")
            print(f"{len(sentences)}: {sentences[-1]}")

        # Optional: Also create version with metadata
        print(f"\nCreating enhanced version with metadata...")
        extract_sentences_with_metadata(
            conllu_directory,
            "enhanced_" + output_filename,
            metadata_filename
        )
        print(f"Enhanced output: enhanced_{output_filename}")
        print(f"Metadata file: {metadata_filename}")

    else:
        print("No sentences were extracted. Please check your directory path and file structure.")


if __name__ == "__main__":
    main()
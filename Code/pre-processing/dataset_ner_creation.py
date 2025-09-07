import random
import os


def file_records_to_list(file_path):
    """Read file line blocks and store them in a list that is returned"""
    with open(file_path, "r", encoding='utf-8') as inf:
        content = inf.read()  # read whole file content as string
    sent_lst = content.split('\n\n')  # split per sentence
    sent_lst = [*filter(None, sent_lst)]  # remove empty units
    return sent_lst


def split_sentences(sentences, train_ratio=0.80, dev_ratio=0.10, test_ratio=0.10):
    """Split sentences into train/dev/test sets"""

    # Shuffle sentences for random distribution
    sentences_copy = sentences.copy()
    random.shuffle(sentences_copy)

    n_total = len(sentences_copy)
    n_train = int(n_total * train_ratio)
    n_dev = int(n_total * dev_ratio)

    # Split the data
    train_sentences = sentences_copy[:n_train]
    dev_sentences = sentences_copy[n_train:n_train + n_dev]
    test_sentences = sentences_copy[n_train + n_dev:]

    return train_sentences, dev_sentences, test_sentences


def save_sentences_to_file(sentences, file_path):
    """Save sentences to file with double newline separation"""
    with open(file_path, 'w', encoding='utf-8') as f:
        for i, sentence in enumerate(sentences):
            f.write(sentence)
            if i < len(sentences) - 1:
                f.write('\n\n')


def main():
    # Set random seed for reproducibility
    random.seed(42)

    # Files
    input_file = "../../Dataset/Testing/final_dataset2.txt"
    train_file = "../../Corpus/train.txt"
    dev_file = "../../Corpus/dev.txt"
    test_file = "../../Corpus/test.txt"

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found!")
        return

    print(f"Reading sentences from {input_file}...")
    sentences = file_records_to_list(input_file)
    total_sentences = len(sentences)

    print(f"Total sentences: {total_sentences}")

    if total_sentences == 0:
        print("No sentences found!")
        return

    # Split the sentences
    train_sentences, dev_sentences, test_sentences = split_sentences(sentences)

    # Print statistics
    print(f"\nSplit results:")
    print(f"Train: {len(train_sentences)} sentences ({len(train_sentences) / total_sentences * 100:.1f}%)")
    print(f"Dev:   {len(dev_sentences)} sentences ({len(dev_sentences) / total_sentences * 100:.1f}%)")
    print(f"Test:  {len(test_sentences)} sentences ({len(test_sentences) / total_sentences * 100:.1f}%)")

    # Save splits
    save_sentences_to_file(train_sentences, train_file)
    save_sentences_to_file(dev_sentences, dev_file)
    save_sentences_to_file(test_sentences, test_file)

    print(f"\n✓ Created {train_file}")
    print(f"✓ Created {dev_file}")
    print(f"✓ Created {test_file}")


if __name__ == "__main__":
    main()
from pathlib import Path
from conllu import parse

def count_sentences_pos(conllu_dir):

    def count_sentences_in_file(file_path):
        try:
            with Path(file_path).open(encoding="utf-8") as f:
                data = f.read()

            sentences = parse(data)
            return len(sentences)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            return 0

    total_sentences = 0

    for i in range(1, 10):
        subdir = conllu_dir / f"{i}Part"
        if subdir.exists() and subdir.is_dir():
            for conllu_file in subdir.glob("*.conllu"):
                count = count_sentences_in_file(conllu_file)
                total_sentences += count

    return total_sentences

def count_sentences_ner(file_path):
    try:
        with Path(file_path).open(encoding="utf-8") as f:
            lines = f.readlines()

        sentence_count = sum(1 for line in lines if line.strip() == "")
        return sentence_count
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return 0

ner_path = Path("../../../Corpus/korpusi.txt")
pos_path = Path("../../../Conllu Files in Corpus/")

ner_sentences = count_sentences_ner(ner_path)
pos_sentences = count_sentences_pos(pos_path)

print(f"NER sentences: {ner_sentences}")
print(f"POS sentences: {pos_sentences}")




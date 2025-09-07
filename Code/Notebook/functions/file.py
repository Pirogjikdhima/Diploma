import csv

def get_lines_from_ner_corpus(path):
    with open(path, 'r', encoding="utf-8") as input_file:
        lines = input_file.readlines()
    return lines

def write_lines_to_csv(lines, output_path):
    with open(output_path, 'w', newline='', encoding="utf-8") as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Sentence #', 'Word', 'NER_Tag'])
        sentence_id = 1
        for line in lines:
            new_line = line.replace("\t\t", "\t").strip().split("\t")

            if not new_line or new_line[0] == '':
                sentence_id += 1

            if len(new_line) >= 2:
                word = new_line[0]
                ner = new_line[1]
                csv_writer.writerow([sentence_id, word, ner])

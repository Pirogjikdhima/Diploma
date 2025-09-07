# import ast
#
# input_file = "../../Dataset/Testing/combined_words.csv"
# output_file = "../../Dataset/Testing/final_dataset.csv"
#
# sentence_end_punct = {".", "!", "?"}
#
# def fix_start_end_offsets(input, output):
#     with open(input, "r", encoding="utf-8") as f_in, open(output, "w", encoding="utf-8") as f_out:
#         cursor = 0
#         sent_id = 0
#
#         for line in f_in:
#             line = line.strip()
#             if not line:
#                 f_out.write("\n")
#                 continue
#
#             # Split line into columns (tab-separated)
#             parts = line.split(",")
#             if len(parts) < 1:
#                 f_out.write(line + "\n")
#                 continue
#
#             token = parts[0]
#
#             # Compute start_char and end_char
#             start_char = cursor
#             end_char = start_char + len(token)
#             cursor = end_char + 1  # assume 1-space separation
#
#             # Update misc column with new offsets
#             misc = parts[-1]
#             try:
#                 misc_dict = ast.literal_eval(misc) if misc and misc != "None" else {}
#             except:
#                 misc_dict = {}
#             misc_dict["start_char"] = str(start_char)
#             misc_dict["end_char"] = str(end_char)
#             parts[-1] = str(misc_dict)
#
#             # Write the updated line
#             f_out.write("\t".join(parts) + "\n")
#
#             # If token is sentence-ending punctuation, reset for next sentence
#             if token in sentence_end_punct:
#                 sent_id += 1
#                 cursor = 0
# def add_blank_lines_after_sentences(input, output):
#     with open(input, "r", encoding="utf-8") as f_in, open(output, "w", encoding="utf-8") as f_out:
#         for line in f_in:
#             stripped = line.strip()
#             if not stripped:
#                 # preserve blank lines
#                 f_out.write("\n")
#                 continue
#
#             f_out.write(line)  # write the current line
#
#             # Get the token (first column)
#             token = stripped.split("\t")[0]
#
#             # If it is sentence-ending punctuation, add a blank line
#             if token in sentence_end_punct:
#                 f_out.write("\n")
#
# fix_start_end_offsets(input_file,output_file)

import csv

file = "../../Dataset/Testing/final_dataset2.txt"

with open(file, "r", encoding="utf-8") as f, \
     open("../../Dataset/Testing/dataset.csv", "w", encoding="utf-8", newline='') as out_f:
    csv_headers = ['word', 'ner_tag', 'lemma', 'upos', 'feats', 'head', 'deprel', 'deps', 'misc']
    csv_writer = csv.writer(out_f)
    csv_writer.writerow(csv_headers)

    for line in f:
        new_line = line.replace("\t\t", '\t').replace("\n", "")
        new_line = new_line.split("\t")
        csv_writer.writerow(new_line)
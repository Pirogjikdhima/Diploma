# from collections import deque
#
# class Ner:
#     def __init__(self, name, tag):
#         self.name = name
#         self.tag = tag
#
#     def __repr__(self):
#         return f"Ner(name={self.name}, tag={self.tag})"
#
# class POS:
#     def __init__(self, name, tag):
#
# ner_queue = deque()
# with open("../../Corpus/korpusi.txt", "r", encoding="utf-8") as file:
#     for line in file:
#         pairs = line.split()
#         if len(pairs) >= 2:
#             ner = Ner(name=pairs[0], tag=pairs[1])
#             queue.append(ner)
#             if pairs[0] == "." and pairs[1] == "O":
#                 queue.append("END")
#
#
# for i in range(len(queue)):
#     if queue:
#         ner = queue.popleft()
#         print(ner)
#     else:
#         print("Queue is empty, no more NERs to process.")
#

from collections import deque
import string


class Ner:
    def __init__(self, name, tag):
        self.name = name
        self.tag = tag

    def __repr__(self):
        return f"Ner(name={self.name}, tag={self.tag})"


def fix_quotes_properly(sentence):
    """Fix quote spacing by tracking opening and closing quotes"""
    import re

    # Find all quote positions and their context
    quote_positions = []
    for match in re.finditer(r'"', sentence):
        quote_positions.append(match.start())

    if len(quote_positions) == 0:
        return sentence

    # Convert to list for easier manipulation
    chars = list(sentence)

    # Process quotes in pairs (assuming they come in pairs)
    for i in range(0, len(quote_positions) - 1, 2):
        if i + 1 >= len(quote_positions):
            break

        opening_pos = quote_positions[i]
        closing_pos = quote_positions[i + 1]

        # Fix opening quote - remove space after it
        if opening_pos + 1 < len(chars) and chars[opening_pos + 1] == ' ':
            chars[opening_pos + 1] = ''  # Mark for removal

        # Fix closing quote - remove space before it
        if closing_pos > 0 and chars[closing_pos - 1] == ' ':
            chars[closing_pos - 1] = ''  # Mark for removal

    # Handle special case: quote at the very beginning of sentence
    if sentence.startswith('" '):
        chars[1] = ''  # Remove space after opening quote at start

    # Handle special case: quote at the very end of sentence
    if len(sentence) >= 2 and sentence.endswith(' "'):
        chars[-2] = ''  # Remove space before closing quote at end

    # Reconstruct sentence without marked characters
    result = ''.join(char for char in chars if char != '')

    return result


def recreate_sentences_with_proper_spacing(input_file, output_line_file):
    queue = deque()

    # Read and parse the input file
    with open(input_file, "r", encoding="utf-8") as file:
        for line in file:
            pairs = line.strip().split()
            if len(pairs) >= 2:
                ner = Ner(name=pairs[0], tag=pairs[1])
                queue.append(ner)
                # Mark sentence end when we see period with O tag
                if pairs[0] == "." and pairs[1] == "O":
                    queue.append("END")

    sentences = []
    current_tokens = []

    # Process tokens to build sentences
    while queue:
        item = queue.popleft()

        if item == "END":
            if current_tokens:
                sentence = format_sentence(current_tokens)
                sentences.append(sentence)
                current_tokens = []
        else:
            current_tokens.append(item.name)

    # Handle any remaining tokens
    if current_tokens:
        sentence = format_sentence(current_tokens)
        sentences.append(sentence)

    # Write to output file
    with open(output_line_file, "w", encoding="utf-8") as outfile:
        for sentence in sentences:
            outfile.write(sentence + "\n")

    print(f"Created {len(sentences)} sentences")
    print(f"Line-separated file: {output_line_file}")

    # Print first few sentences as examples
    print("\nFirst few sentences:")
    for i, sentence in enumerate(sentences[:3]):
        print(f"{i + 1}: {sentence}")

    return sentences


def format_sentence(tokens):
    """Format tokens into a proper sentence with correct spacing"""
    if not tokens:
        return ""

    # First, join and handle special patterns with regex
    raw_sentence = " ".join(tokens)

    # Handle /{word}or{number}/ patterns
    import re
    pattern = r'/ ([^/]+) /'
    formatted_sentence = re.sub(pattern, r'/\1/', raw_sentence)

    # Handle date patterns like: 31 /12/ 2021 -> 31/12/2021
    date_pattern = r'(\d+)\s+(/\d+/)\s+(\d+)'
    formatted_sentence = re.sub(date_pattern, r'\1\2\3', formatted_sentence)

    # Handle partial date patterns
    partial_date_pattern = r'(/\d+/)\s+(\d+)'
    formatted_sentence = re.sub(partial_date_pattern, r'\1\2', formatted_sentence)

    partial_date_pattern2 = r'(\d+)\s+(/\d+/)'
    formatted_sentence = re.sub(partial_date_pattern2, r'\1\2', formatted_sentence)

    # Now apply other formatting rules
    result = formatted_sentence

    # Remove spaces before common punctuation
    punctuation_to_fix = ['.', ',', '!', '?', ';', ':', ')', ']', '}']
    for punct in punctuation_to_fix:
        result = result.replace(f' {punct}', punct)

    # Remove spaces after opening punctuation
    opening_punct = ['(', '[', '{']
    for punct in opening_punct:
        result = result.replace(f'{punct} ', punct)

    # # Handle quotes at the beginning and end
    # if result.startswith('" '):
    #     result = '"' + result[2:]
    #
    # if result.endswith(' "'):
    #     result = result[:-2] + '"'
    #
    # # Handle quotes in middle
    # result = result.replace(' " ', '"')
    result = fix_quotes_properly(result)

    return result


# Alternative simpler approach - just handle common punctuation
# Usage example:
if __name__ == "__main__":
    sentences = recreate_sentences_with_proper_spacing(
        "../../Corpus/korpusi.txt",
        "../../Testing/Sentences/corpus_sentences_formatted.txt"
    )
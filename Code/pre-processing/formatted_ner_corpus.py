from pathlib import Path


def format_ner_file(input_file, output_file):
    """
    Format NER file so that each tag is exactly 2 tabs away from the word
    """

    # Create output directory if it doesn't exist
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as f_input, \
            open(output_file, 'w', encoding='utf-8') as f_output:

        for line in f_input:
            line = line.strip()

            # Skip empty lines
            if not line:
                f_output.write('\n')
                continue

            # Split by any whitespace/tabs
            parts = line.split()

            if len(parts) >= 2:
                word = parts[0]
                tag = parts[1]

                # Format with exactly 2 tabs between word and tag
                formatted_line = f"{word}\t\t{tag}\n"
                f_output.write(formatted_line)
            else:
                # If line doesn't have both word and tag, keep as is
                f_output.write(f"{line}\n")


def main():
    # Input and output file paths
    input_file = "../../Corpus/korpusi.txt"  # Your original NER file
    output_file = "../../Corpus/korpusi.txt"  # Formatted output

    print("Formatting NER file...")
    format_ner_file(input_file, output_file)
    print(f"Formatted NER file saved to: {output_file}")

    # Show a sample of the formatted file
    print("\nSample of formatted file:")
    with open(output_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 10:  # Show first 10 lines
                print(repr(line))  # repr shows tabs as \t
            else:
                break


if __name__ == "__main__":
    main()
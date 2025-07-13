import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def load_ner_pos_data(csv_file="combined_words.csv"):
    """Load the NER-POS matched data into a pandas DataFrame"""
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Convert match_score to numeric (handling empty values)
    df['match_score'] = pd.to_numeric(df['match_score'], errors='coerce')

    print(f"Loaded {len(df)} matched words with NER and POS information")
    return df


def analyze_ner_distribution(df):
    """Analyze the distribution of NER tags"""
    # Count by NER tag
    ner_counts = df['ner_tag'].value_counts()

    print("NER Tag Distribution:")
    print(ner_counts)

    # Create a pie chart
    plt.figure(figsize=(10, 8))
    plt.pie(ner_counts[:10], labels=ner_counts.index[:10], autopct='%1.1f%%')
    plt.title('Top 10 NER Tags Distribution')
    plt.savefig('ner_distribution.png')
    plt.close()

    return ner_counts


def analyze_match_types(df):
    """Analyze the distribution of match types"""
    match_counts = df['match_type'].value_counts()

    print("Match Type Distribution:")
    print(match_counts)

    # Create a bar chart
    plt.figure(figsize=(8, 6))
    sns.barplot(x=match_counts.index, y=match_counts.values)
    plt.title('Match Type Distribution')
    plt.ylabel('Count')
    plt.savefig('match_type_distribution.png')
    plt.close()

    return match_counts


def analyze_pos_distribution(df):
    """Analyze the distribution of POS tags"""
    pos_counts = df['pos_tag'].value_counts()

    print("POS Tag Distribution:")
    print(pos_counts)

    # Create a bar chart for top 10 POS tags
    plt.figure(figsize=(12, 6))
    sns.barplot(x=pos_counts.index[:10], y=pos_counts.values[:10])
    plt.title('Top 10 POS Tags Distribution')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.savefig('pos_distribution.png')
    plt.close()

    return pos_counts


def analyze_ner_pos_relationship(df):
    """Analyze the relationship between NER and POS tags"""
    # Create a cross-tabulation
    ner_pos_cross = pd.crosstab(df['ner_tag'], df['pos_tag'])

    # Get the top 8 NER tags and top 8 POS tags for a cleaner heatmap
    top_ner = df['ner_tag'].value_counts().nlargest(8).index
    top_pos = df['pos_tag'].value_counts().nlargest(8).index

    # Filter the cross-tabulation
    ner_pos_filtered = ner_pos_cross.loc[top_ner, top_pos]

    # Create a heatmap
    plt.figure(figsize=(12, 10))
    sns.heatmap(ner_pos_filtered, cmap='YlGnBu', annot=True, fmt='d')
    plt.title('Relationship Between NER and POS Tags')
    plt.savefig('ner_pos_relationship.png')
    plt.close()

    return ner_pos_cross


def main():
    # Load the data
    df = load_ner_pos_data()

    # Display basic information
    print("\nDataFrame Info:")
    print(df.info())
    print("\nSample Data:")
    print(df.head())

    # Perform analyses
    analyze_ner_distribution(df)
    analyze_match_types(df)
    analyze_pos_distribution(df)
    analyze_ner_pos_relationship(df)

    # Export cleansed data
    df.to_csv('ner_pos_analyzed.csv', index=False)
    print("\nAnalysis complete. Visualizations saved as PNG files.")


if __name__ == "__main__":
    main()
import ast

import pandas as pd


def safe_literal_eval(x):
    try:
        return ast.literal_eval(x)
    except:
        return {}


df = pd.read_csv('../../Dataset/Testing/combined_words.csv')

pos_split = df['pos'].str.split('|', expand=True)
df['lemma'] = pos_split[1]
df['pos_tag'] = pos_split[2]
df['morph'] = pos_split[3].fillna('{}')

df['morph'] = df['morph'].apply(safe_literal_eval)

morph_df = pd.json_normalize(df['morph'])

flattened_df = pd.DataFrame({
    'Word': df['word'],
    'Lemma': df['lemma'],
    'NER_Tag': df['ner'],
    'POS_Tag': df['pos_tag']
})
flattened_df = pd.concat([flattened_df, morph_df], axis=1)

morph_df = df = pos_split = None

print(flattened_df.head())

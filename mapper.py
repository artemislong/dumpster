# mapper.py

#!/usr/bin/env python
import sys, re, os
import string

def word_scores():
    score_file = os.path.join(os.path.abspath('.'), 'AFINN-en-165.txt')
    word_scores = {}
    with open(score_file, 'r') as file:
        for line in file:
            word, score = line.strip().split('\t')
            score = int(score)
            word_scores[word] = score
    return word_scores

# reads line and only keeps words, removing other characters
def read_and_clean_text():
  text = sys.stdin.readline()
  text = text.lower()
  text = re.sub('\[.*?\]', '', text)
  text = re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
  text = re.sub('[\d\n]', ' ', text)
  return text

# the mapper function extracts every 
def main(argv):
    line = read_and_clean_text()
    pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*") # define word pattern
    # read valtable
    val_table = word_scores()
    # read president name
    current_file = os.environ["map_input_file"]
    president = current_file.split("_")[0]
    try:
        while line:
            print(f"Processing president: {president}, Line: {line.strip()}")
            # extracting every word
            for word in pattern.findall(line):
                valence = val_table.get(word, 0)
                print(f"{president}\t{valence}")
            line = read_and_clean_text()
    except EOFError as error:
        return None

if __name__ == "__main__":
    main(sys.argv)

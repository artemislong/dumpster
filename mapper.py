#!/usr/bin/env python
import sys, re, os
import string

def word_scores():
    scores = {'abandon': -2, 'abandoned': -2}
    return scores

# reads line and only keeps words, removing other characters
def read_and_clean_text(text):
  text = text.lower()
  text = re.sub('\[.*?\]', '', text)
  text = re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
  text = re.sub('[\d\n]', ' ', text)
  return text

# the mapper function extracts every 
def main(argv):
    pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*") # define word pattern
    # read valtable
    score_dict = word_scores()
    val_table = score_dict
    # read president name
    # Use a regular expression to isolate the first word before an underscore in the file name
    president_name = "unknown"
    president_name = re.search(r'^([^_]+)_', file_name)
    # read and process line    
    line_raw = sys.stdin.readline()
    line = read_and_clean_text(line_raw)
    try:
        while line:
            if ".txt" in line:
                # Use a regular expression to extract the first word before an underscore
                match = re.search(r'(\w+)_\w+_\d+\.txt', line)
                if match:
                    president_name = match.group(1)
            else:    
                line = read_and_clean_text(line)
                # print(f"Processing president: {president_name}, Line: {line.strip()}")
               	# extracting every word
               	for word in pattern.findall(line):
               	    valence = val_table.get(word, 0)
               	    print(f"{president_name}\t{valence}")
       	    line_raw = sys.stdin.readline()
       	    line = read_and_clean_text(line_raw)
    except EOFError as error:
        return None

if __name__ == "__main__":
    main(sys.argv)

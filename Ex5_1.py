from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")

# Find out the number of occurences of each word in a text file
class WordCount(MRJob):

    # Mapper initialized, so words can be called in other functions in class
    def mapper_init_get_words(self):
        self.words = {}

    # Set the word to a lower case and default 0, then add 1 for a found word
    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            word = word.lower()
            self.words.setdefault(word, 0)
            self.words[word] = self.words[word] + 1

    # Outputs tuples of key(or word) and value
    def mapper_final_get_words(self):
        for word, val in self.words.items():
            yield word, val

    # Sums up the words
    def sum_of_words(self, word, counts):
        yield word, sum(counts)

    # Override the steps, since mapper is three folded
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init_get_words,
                       mapper=self.mapper_get_words,
                       mapper_final=self.mapper_final_get_words,
                       reducer=self.sum_of_words)]

if __name__ == '__main__':
    WordCount.run()
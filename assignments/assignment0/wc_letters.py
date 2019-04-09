import re
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])

def get_valid_words(line):
    """
    Returns list of words which start with an alphanumeric character.

    """
    valid_words = []
    for w in re.split(r'[^\w]+', line):
        if len(w) > 0 and str.isalpha(w[0]):
            valid_words.append(str.lower(w))

    return valid_words


words = lines.flatMap(get_valid_words)
pairs = words.map(lambda w: (w[0], 1))
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)

counts.saveAsTextFile(sys.argv[2])
sc.stop()

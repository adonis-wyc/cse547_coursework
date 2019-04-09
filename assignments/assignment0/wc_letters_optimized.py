from pyspark import SparkConf, SparkContext

from collections import defaultdict

import re
import sys

conf = SparkConf()
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1])

def get_start_letter_counts(line):
    """
    Returns list of words which start with an alphanumeric character.

    """
    start_letter_counts = defaultdict(int)
    for w in re.split(r'[^\w]+', line):
        if len(w) > 0 and str.isalpha(w[0]):
            start_letter_counts[str.lower(w)[0]] += 1

    return list(start_letter_counts.items())


filelevel_letter_counts = lines.flatMap(get_start_letter_counts)
pairs = filelevel_letter_counts.map(lambda tup: (tup[0], tup[1]))
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)

counts.coalesce(1).saveAsTextFile(sys.argv[2])
sc.stop()

from collections import defaultdict

import itertools
import sys

class AssociationRule(object):
    def __init__(self, lhs, rhs, confidence):
        self.lhs = lhs
        self.rhs = rhs
        self.confidence = confidence

    def __str__(self):
        return "%s -> %s (conf: %s)" % (str(self.lhs), str(self.rhs), self.confidence)


def _get_frequent_singleton_counts(input_file, min_support):
    c1_counts = defaultdict(int)
    with open(input_file) as fp:
        for line in fp:
            basket = line.strip().split(" ")
            for item in basket:
                c1_counts[item] += 1

    frequent_singleton_counts = {k: v for k, v in c1_counts.items() if v >= min_support}
    return frequent_singleton_counts


def _get_frequent_pair_counts(input_file, min_support, old_id_to_new_id_map):
    c2_counts = defaultdict(int)
    with open(input_file) as fp:
        for line in fp:
            basket = line.strip().split(" ")
            # The old_id_to_new_id_map contains only the mapping for frequent items.
            basket = sorted([old_id_to_new_id_map[item] for item in basket if item in old_id_to_new_id_map])

            for pair in itertools.combinations(basket, 2):
                c2_counts[pair] += 1

    frequent_pair_counts = {k: v for k, v in c2_counts.items() if v >= min_support}
    return frequent_pair_counts


def _get_frequent_triple_counts(input_file, min_support, old_id_to_new_id_map, frequent_pairs):
    c3_counts = defaultdict(int)
    with open(input_file) as fp:
        for line in fp:
            basket = line.strip().split(" ")
            # The old_id_to_new_id_map contains only the mapping for frequent items.
            basket = sorted([old_id_to_new_id_map[item] for item in basket if item in old_id_to_new_id_map])

            for triple in itertools.combinations(basket, 3):
                if all([pair in frequent_pairs for pair in itertools.combinations(triple, 2)]):
                    c3_counts[triple] += 1

    frequent_triple_counts = {k: v for k, v in c3_counts.items() if v >= min_support}
    return frequent_triple_counts


def _get_pair_rules_with_confidence(frequent_pair_counts, frequent_singleton_counts, new_id_to_old_id_map):
    pair_rules = []
    for pair, count in frequent_pair_counts.items():
        x, y = pair
        pair_rules.append(AssociationRule(new_id_to_old_id_map[x], new_id_to_old_id_map[y], count / frequent_singleton_counts[x]))
        pair_rules.append(AssociationRule(new_id_to_old_id_map[y], new_id_to_old_id_map[x], count / frequent_singleton_counts[y]))

    return sorted(pair_rules, key=lambda association_rule: association_rule.confidence, reverse=True)


def _get_triple_rules_with_confidence(frequent_triple_counts, frequent_pair_counts, new_id_to_old_id_map):
    triple_rules = []
    for triple, count in frequent_triple_counts.items():
        x, y, z = triple
        triple_rules.append(AssociationRule((new_id_to_old_id_map[x], new_id_to_old_id_map[y]), new_id_to_old_id_map[z], count / frequent_pair_counts[(x, y)]))
        triple_rules.append(AssociationRule((new_id_to_old_id_map[x], new_id_to_old_id_map[z]), new_id_to_old_id_map[y], count / frequent_pair_counts[(x, z)]))
        triple_rules.append(AssociationRule((new_id_to_old_id_map[y], new_id_to_old_id_map[z]), new_id_to_old_id_map[x], count / frequent_pair_counts[(y, z)]))

    return sorted(triple_rules, key=lambda association_rule: association_rule.confidence, reverse=True)


if __name__ == "__main__":
    min_support = 100
    input_file = sys.argv[1] # "./data/browsing.txt"

    frequent_singleton_counts = _get_frequent_singleton_counts(input_file, min_support)

    # Map string IDs to integer to IDs only for frequent items.
    old_id_to_new_id_map = {}
    new_id_to_old_id_map = {}
    new_id = 0
    for old_id in frequent_singleton_counts.keys():
        new_id += 1
        new_id_to_old_id_map[new_id] = old_id
        old_id_to_new_id_map[old_id] = new_id

    frequent_singleton_counts = {old_id_to_new_id_map[k]: v for k, v in frequent_singleton_counts.items()}
    #print(len(frequent_singleton_counts))

    # Get frequent pairs from frequent_singletons using second pass on data.
    frequent_pair_counts = _get_frequent_pair_counts(input_file, min_support, old_id_to_new_id_map)
    #print(len(frequent_pair_counts))

    # Get frequent triples from frequent_pairs using third pass on data.
    frequent_triple_counts = _get_frequent_triple_counts(input_file, min_support, old_id_to_new_id_map, frequent_pair_counts)
    #print(len(frequent_triple_counts))

    # Generate rules with confidence scores: ((lhs, rhs), confidence)
    pair_rules = _get_pair_rules_with_confidence(frequent_pair_counts, frequent_singleton_counts, new_id_to_old_id_map)
    triple_rules = _get_triple_rules_with_confidence(frequent_triple_counts, frequent_pair_counts, new_id_to_old_id_map)

    print("\nTop 5 pair rules:")
    for rule in pair_rules[:5]:
        print(rule)

    print("\nTop 5 triple rules:")
    for rule in triple_rules[:5]:
        print(rule)

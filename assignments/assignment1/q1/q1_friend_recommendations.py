from pyspark import SparkConf, SparkContext

from collections import defaultdict

import operator
import re
import sys

conf = SparkConf()
sc = SparkContext(conf=conf)

input_file = sys.argv[1]
output_dir = sys.argv[2]

lines = sc.textFile(input_file)

def get_user_friend_pairs(line):
    toks = re.split(r'\t+', line)
    user = int(toks[0])
    friends_str = toks[1]

    if not friends_str:
        return []

    friends = [int(x) for x in friends_str.split(",")]

    if not friends_str:
        return []

    return [(user, friend) for friend in friends]


# This contains every user <-> friend pair.
user_friends = lines.flatMap(get_user_friend_pairs)

# Every user needs the friends of each of the user's friends.
# In other words, since every friend also has this user in their list of friends (bidirectional),
# each of this user, would want to see all the friends of this user.

# This contains, each (friend, list of user's friends)
def get_second_order_friends(line):
    toks = re.split(r'\t+', line)
    user = int(toks[0])
    friends_str = toks[1]

    if not friends_str:
        return []

    friends = [int(x) for x in friends_str.split(",")]

    result_list = []
    for friend in friends:
        result_list.append((friend, friends))

    return result_list


second_order_friends = lines.flatMap(get_second_order_friends)

# Now, we can append all second order friends into a single list.
# Then we can count the number of times each friend occurs (this is the number of times
# this friend occurs in second order friends; like number of documents a unique word is present in).
# However, we need to filter out the user, and user's first-order friends, because we dont want
# to recommend those who are already friends. Spark provides, nice reduce-like
# operation "cogroup" which can help us do filter and count.
# Further, once the candidate mutual friends for recommendation has been created,
# we would sort it based on number of mutual friends (secondary key being the user id).
cogrouped_mutual_friends = second_order_friends.cogroup(user_friends)

def filter_count_sort(x):
    user, cogrouped_value = x
    second_order_friends, own_friends = cogrouped_value
    second_ordered_friends_flattened = \
        [friend for cur_second_order_friends in second_order_friends for friend in cur_second_order_friends]

    # filter and count
    filtered_mutual_friends = defaultdict(int)
    for f in second_ordered_friends_flattened:
        if f != user and f not in own_friends:
            filtered_mutual_friends[f] += 1

    # sort
    secondary_sorted = sorted(filtered_mutual_friends.items(), key=operator.itemgetter(0))
    sorted_mutual_friends = sorted(secondary_sorted, key=operator.itemgetter(1), reverse=True)

    num_recommendations = 10
    return (user, sorted_mutual_friends[:num_recommendations])


mutual_friends_ordered = cogrouped_mutual_friends.map(filter_count_sort)

# For recommendations, we do not need the counts.
non_zero_friends_user_recommendations = \
    mutual_friends_ordered.map(lambda k_v: (k_v[0], [friend_count[0] for friend_count in k_v[1]]))

# We can process the users without friends separately.
def process_zero_friends_users(line):
    toks = re.split(r'\t+', line)
    user = int(toks[0])
    friends_str = toks[1]

    if friends_str:
        return []

    return [(user, [])]


# This contains every user <-> friend pair.
zero_friends_user_recommendations = lines.flatMap(process_zero_friends_users)

# We can take union to a single RDD of recommendations.
recommendations = non_zero_friends_user_recommendations.union(zero_friends_user_recommendations)

# For each of seeing the output, sorting the recommendations by user id. This may not be required,
# and it might save considerable amount of time if skipped.
#recommendations = recommendations.sortByKey() # This can uncommented to sort the final output by key.

# Formatting and saving the output.
formatted_recommendations = recommendations.map(lambda x: "".join([str(x[0]), "\t", ",".join(map(str, x[1]))]))
formatted_recommendations.coalesce(1).saveAsTextFile(output_dir)
sc.stop()

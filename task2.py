import collections
import sys
import csv
import time
from itertools import combinations
from pyspark import SparkContext

# preprocessing
raw_input_file_path = 'ta_feng_all_months_merged.csv'
process_file_path = 'date_customer_producer.csv'
sc = SparkContext.getOrCreate()
text_rdd = sc.textFile(raw_input_file_path)
first_line = text_rdd.first()

date_customer_producer_rdd = text_rdd.filter(lambda x : x != first_line).map(lambda x : (x.split(',')[0], x.split(',')[1], x.split(',')[5])).map(lambda pair : (pair[0][1:-1]+'-'+str(int(pair[1][1:-1])), int(pair[2][1:-1])))

with open(process_file_path, 'w') as output:
    writer = csv.writer(output, quoting=csv.QUOTE_NONE)
    writer.writerow(["DATE-CUSTOMER_ID", "PRODUCT_ID"])
    for row in date_customer_producer_rdd.collect():
        writer.writerow(row)
    output.close()


# task2
def renew_Candidate(preCandidate_list):
    if preCandidate_list is not None and len(preCandidate_list) > 0:
        res = []
        if type(preCandidate_list[0]) == str:
            for pair in combinations(preCandidate_list, 2):
                res.append(pair)
        else:
            for i in range(len(preCandidate_list)-1):
                base_tuple = preCandidate_list[i]
                for appender in preCandidate_list[i+1:]:
                    if base_tuple[:-1] == appender[:-1]:
                        new_tuple = tuple(sorted(list(set(base_tuple).union(set(appender)))))
                        res.append(new_tuple)
                    else:
                        break
        return res


def get_candidate_itemsets(subset, support, total_data_size):
    if subset is None:
        return
    baskets_list = list(subset)
    scaled_down_sp = support * float(len(baskets_list) / total_data_size)
    single_item_dict = {}
    final_dict = {}
    for basket in baskets_list:
        for item in basket:
            if item not in single_item_dict.keys():
                single_item_dict[item] = 1
            else:
                single_item_dict[item] += 1
    filtered_item_dict = dict(filter(lambda item_count : item_count[1] >= scaled_down_sp, single_item_dict.items()))
    frequent_single_item_list = list(filtered_item_dict.keys())
    candidate_list = frequent_single_item_list
    curLen = 1
    while candidate_list is not None and len(candidate_list)>0:
        item_count_dict = {}
        for basket in baskets_list:
            basket = list(set(basket).intersection(set(frequent_single_item_list)))
            for candidate in candidate_list:
                curSet = set()
                if type(candidate) == str:
                    curSet.add(candidate)
                else:
                    curSet = set(candidate)
                if curSet.issubset(set(basket)):
                    if candidate not in item_count_dict.keys():
                        item_count_dict[candidate] = 1
                    else:
                        item_count_dict[candidate] += 1
        filtered_item_dict = dict(filter(lambda item_count: item_count[1] >= scaled_down_sp, item_count_dict.items()))
        final_dict[curLen] = list(filtered_item_dict.keys())
        curLen += 1
        candidate_list = renew_Candidate(sorted(list(filtered_item_dict.keys())))
    return final_dict.values()

def count_itemsets(subset,candidates):
    item_count_dict = {}
    baskets_set = set()
    basket = list(subset)
    for ele in basket:
        baskets_set.add(ele)
    for candidate in candidates:
        curSet = set()
        if type(candidate) == str:
            curSet.add(candidate)
        else:
            curSet = set(candidate)
        if curSet.issubset(baskets_set):
            if candidate not in item_count_dict.keys():
                item_count_dict[candidate] = 1
            else:
                item_count_dict[candidate] += 1
    return item_count_dict.items()

def normalize(lists):
    res_dict = collections.defaultdict(list)
    for item in lists:
        if type(item) == str:
            item = tuple(item.split(','))
            item = str(str(item)[:-2] + ')')
            res_dict[1].append(item)
        else:
            item = sorted(item)
            value = str(tuple(item))
            res_dict[len(item)].append(value)

    for k,v in res_dict.items():
        res_dict[k] = sorted(v)
    return res_dict

start_time = time.clock()
filter_threshold = 20
support_num = 50
input_file_path = "date_customer_producer.csv"
output_file_path = "output2.txt"
text_rdd = sc.textFile(input_file_path,6)
first_line = text_rdd.first()
baskets_rdd = None

user_business_rdd = text_rdd.filter(lambda x : x != first_line and x != "").map(lambda x : (x.split(',')[0], x.split(',')[1])).groupByKey().mapValues(list).filter(lambda customer_product : len(customer_product[1]) > filter_threshold)
baskets_rdd = user_business_rdd.map(lambda x : x[1])

data_size = baskets_rdd.count()
candidates_subsets_rdd = baskets_rdd.mapPartitions(lambda subset : get_candidate_itemsets(subset=subset, support=support_num, total_data_size=data_size))
tmp_candidates_lists = candidates_subsets_rdd.flatMap(lambda element : element).distinct().collect()
tmp_frequent_lists = baskets_rdd.flatMap(lambda subset : count_itemsets(subset=subset, candidates=tmp_candidates_lists)).reduceByKey(lambda x,y : x+y).filter(lambda candidate_count: candidate_count[1] >= support_num).map(lambda x: x[0]).collect()

with open(output_file_path, 'w') as output:
    output.write('Candidates:\n')
    for v in normalize(tmp_candidates_lists).values():
        s = ''
        for item in v:
            s += ','+item
        s += '\n'
        output.write(s[1:])
    output.write('\nFrequent Itemsets:\n')
    for v in normalize(tmp_frequent_lists).values():
        s = ''
        for item in v:
            s += ',' + item
        s += '\n'
        output.write(s[1:])
    output.close()

endtime = time.clock()
execution_time = endtime - start_time
print('Duration: ',execution_time)



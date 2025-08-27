import time
import concurrent.futures
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from statistics import mean, stdev

# Configuration
# ES_HOST = 'https://ajmal:eGkbtw6Xf8dpBQr@dev.es.om.digitalmall.app' 
# INDEX_NAME = 'dm_dev_global_search_v3' 
ES_HOST = 'https://ajmal:zP4ADp3sSJpe2dV@es.om.digitalmall.app' 
INDEX_NAME = 'dm_production_global_search_v3' 
NUM_REQUESTS = 10  # Number of requests to send
CONCURRENCY = 10  # Number of concurrent requests
QUERY_TERMS = ['اكيورا', 'اكيور', 'اكي', 'acura']  # Query terms to test (rotate through them)
LANG = 'ar'  # Language for Arabic filters; change to 'en' if needed

# Normalize Arabic function (from your JS code)
def normalize_arabic(text):
    # Implement your normalization logic here; this is a placeholder based on common Arabic normalization
    # Adjust as per your exact normalizeArabic function
    # Example: Remove diacritics, normalize alif variants, etc.
    replacements = {
        '\u0622': '\u0627',  # Alif maddah to alif
        '\u0623': '\u0627',  # Alif hamza above to alif
        '\u0625': '\u0627',  # Alif hamza below to alif
        '\u0629': '\u0647',  # Teh marbuta to heh
        '\u0649': '\u064A',  # Alef maksura to yeh
        '\u0624': '\u0648',  # Waw hamza to waw
        '\u0626': '\u064A',  # Yeh hamza to yeh
    }
    return ''.join(replacements.get(c, c) for c in text)

# Build the query body
def build_query(query, lang):
    arabic_search_filters = []
    if lang == "ar":
        query = normalize_arabic(query)
        arabic_search_filters = [
            {
                "match": {
                    "tags_primary.arabic": {
                        "query": query,
                        "boost": 10000,
                    },
                },
            },
            {
                "match_phrase": {
                    "tags_primary.arabic": {
                        "query": query,
                        "boost": 3000,
                        "slop": 2,
                    },
                },
            },
            {
                "match": {
                    "tags_secondary.arabic": {
                        "query": query,
                        "boost": 6000,
                    },
                },
            },
            {
                "match": {
                    "tags_tertiary.arabic": {
                        "query": query,
                        "boost": 1000,
                    },
                },
            },
            {
                "match": {
                    "tags_primary.arabic": {
                        "query": query,
                        "fuzziness": "AUTO:4,8",
                        "boost": 100,
                    },
                },
            },
            {
                "match": {
                    "tags_secondary.arabic": {
                        "query": query,
                        "fuzziness": "AUTO:4,8",
                        "boost": 50,
                    },
                },
            },
        ]

    return {
        "query": {
            "bool": {
                "should": [
                    # Exact matches for the input query
                    {
                        "term": {
                            "tags_primary.exact": {
                                "value": query,
                                "boost": 15000,
                            },
                        },
                    },
                    {
                        "term": {
                            "tags_secondary.exact": {
                                "value": query,
                                "boost": 10000,
                            },
                        },
                    },
                    {
                        "term": {
                            "tags_tertiary.exact": {
                                "value": query,
                                "boost": 1000,
                            },
                        },
                    },
                    # English prefix and phrase matches
                    {
                        "match": {
                            "tags_primary": {
                                "query": query,
                                "boost": 5000,
                            },
                        },
                    },
                    {
                        "match_phrase": {
                            "tags_secondary": {
                                "query": query,
                                "boost": 3000,
                            },
                        },
                    },
                    {
                        "match_phrase": {
                            "tags_tertiary": {
                                "query": query,
                                "boost": 1000,
                            },
                        },
                    },
                    *arabic_search_filters,
                    # Fuzzy matches for robustness
                    {
                        "match": {
                            "tags_primary.fuzzy": {
                                "query": query,
                                "fuzziness": "AUTO:4,8",
                                "boost": 100,
                            },
                        },
                    },
                    {
                        "match": {
                            "tags_secondary.fuzzy": {
                                "query": query,
                                "fuzziness": "AUTO:4,8",
                                "boost": 50,
                            },
                        },
                    },
                    {
                        "match": {
                            "tags_tertiary.fuzzy": {
                                "query": query,
                                "fuzziness": "2",
                                "boost": 25,
                            },
                        },
                    },
                ],
                "minimum_should_match": 1,
            },
        },
        "sort": [
            { "_score": "desc" },
            { "department_sort_order": "asc" },
            { "title.en.keyword": { "order": "asc", "missing": "_last" } },
        ],
        "size": 30,
    }

# Elasticsearch client
client = Elasticsearch(ES_HOST)

# Function to execute a single search
def execute_search(query_term, lang):
    start_time = time.time()
    try:
        query_body = build_query(query_term, lang)
        response = client.search(
            index=INDEX_NAME,
            body=query_body
        )
        duration = time.time() - start_time
        return {
            "status": "success",
            "hits": response['hits']['total']['value'],
            "duration": duration
        }
    except ApiError as e:
        duration = time.time() - start_time
        return {
            "status": "error",
            "error": str(e),
            "duration": duration
        }

# Load test function
def load_test():
    response_times = []
    successes = 0
    errors = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = []
        for i in range(NUM_REQUESTS):
            query_term = QUERY_TERMS[i % len(QUERY_TERMS)]  # Rotate through query terms
            futures.append(executor.submit(execute_search, query_term, LANG))
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            response_times.append(result['duration'])
            if result['status'] == "success":
                successes += 1
            else:
                errors += 1
                print(f"Error: {result['error']}")

    print(f"Total requests: {NUM_REQUESTS}")
    print(f"Successes: {successes}")
    print(f"Errors: {errors}")
    if response_times:
        avg_time = mean(response_times)
        std_dev = stdev(response_times) if len(response_times) > 1 else 0
        print(f"Average response time: {avg_time:.4f} seconds")
        print(f"Standard deviation: {std_dev:.4f} seconds")
        print(f"Min response time: {min(response_times):.4f} seconds")
        print(f"Max response time: {max(response_times):.4f} seconds")
    else:
        print("No successful responses.")

# Run the load test
load_test()
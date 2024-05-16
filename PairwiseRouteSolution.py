import json
import time
from tqdm import tqdm
import ijson
import pandas as pd


def split_route(route_data):
    routes = []
    merchandise = []

    for trip in route_data:
        route = f"{trip['from']}-{trip['to']}"
        routes.append(route)
        merchandise.append(trip['merchandise'])

    return routes, merchandise


def lcs_similarity(actual_route, standard_route, actual_merch, standard_merch):
    matrix = [[0] * (len(standard_route) + 1) for _ in range(len(actual_route) + 1)]

    # Fill the matrix
    for i in range(1, len(actual_route) + 1):
        for j in range(1, len(standard_route) + 1):
            if actual_route[i - 1] == standard_route[j - 1]:
                matrix[i][j] = matrix[i - 1][j - 1] + 1
            else:
                matrix[i][j] = max(int(matrix[i - 1][j]), int(matrix[i][j - 1]))

    # read the matrix
    i = len(actual_route)
    j = len(standard_route)

    matching_indices = []

    while i > 0 and j > 0:
        if actual_route[i - 1] == standard_route[j - 1]:
            matching_indices.append((i - 1, j - 1))
            i -= 1
            j -= 1
        elif matrix[i - 1][j] > matrix[i][j - 1]:
            i -= 1
        else:
            j -= 1

    matching_indices.reverse()

    diff_merch = 0.01
    total_merch = 1

    for i, j in matching_indices:
        for merch in actual_merch[i]:
            total_merch += 1  # count total products in actual
            if merch in standard_merch[j]:
                diff_merch += 1  # count same products
                diff_merch -= abs(actual_merch[i][merch] - standard_merch[j][merch]) / actual_merch[i][
                    merch]  # penalize difference proportionally

    lcs = float(matrix[-1][-1] / len(set(actual_route).union(set(standard_route))))
    merch = diff_merch / total_merch

    return lcs + merch


most_similar_element = {}

with open("./data/standard_routes.json", "r") as file:
    standard_routes = json.load(file)

beginTime = time.perf_counter()

for standard_route in tqdm(standard_routes):
    standard_route_id = standard_route['id']
    current_trips = standard_route['route']

    standard_route, standard_merch = split_route(current_trips)

    # Compare the current route with all other routes
    with open("./data/actual_routes.json", "r") as file:
        for actual_route in ijson.items(file, "item"):
            actual_id = actual_route['id']
            actual_trips = actual_route['route']

            actual_route, actual_merch = split_route(actual_trips)

            route_similarity = lcs_similarity(standard_route, actual_route, standard_route, actual_merch)

            max_similarity = 0

            if actual_id in most_similar_element:
                max_similarity = most_similar_element[actual_id]
                max_similarity = max_similarity[1]

            if route_similarity >= max_similarity:
                most_similar_element[actual_id] = (standard_route_id, route_similarity)

print(f"Done in {time.perf_counter() - beginTime}")
results_df = pd.DataFrame.from_records([(key, value[0]) for key, value in most_similar_element.items()],
                                       columns=['actual_route_id', 'standard_route_id'])
results_df = results_df.sort_values(by=["actual_route_id"], ascending=True)
results_df.to_csv("results_pairwise.csv", index=False)

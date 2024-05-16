import argparse
import json
import os
import random

import pandas as pd

from copy import deepcopy

from tqdm import tqdm



def generate_merchandise():
    merchandise = {}
    num_products = random.randint(1, 4)

    for _ in range(num_products):
        product = random.choice(products)
        quantity = random.randint(1, 40)
        merchandise[product] = quantity

    return merchandise


def prepend_mutation_route(nodes, city):
    from_city = city
    to_city = nodes[0]['from']

    return {
        "from": from_city,
        "to": to_city,
        "merchandise": generate_merchandise()
    }


def append_mutation_route(nodes, city):
    from_city = nodes[-1]['to']
    to_city = city

    return {
        "from": from_city,
        "to": to_city,
        "merchandise": generate_merchandise()
    }


def insert_mutation_route(modified_route, insert_index, new_entry_city):
    from_city = modified_route[insert_index - 1]['to']
    to_city = modified_route[insert_index + 1]["from"]

    from_node = {
        "from": from_city,
        "to": new_entry_city,
        "merchandise": generate_merchandise()
    }

    to_node = {
        "from": new_entry_city,
        "to": to_city,
        "merchandise": generate_merchandise()
    }

    return [from_node, to_node]


def deletion_trip(route, index):
    trip = route[index]

    if index == 0 or index == len(route) - 1:
        route.remove(trip)
        return

    route[index - 1]['to'] = route[index + 1]['from']
    route.remove(trip)


def variate_product_quantities(merchandise):
    for product, quantity in merchandise.items():
        if random.random() <= configs['generation']['quantity']['probability']:
            variance = random.randint(-configs['generation']['quantity']['variance'],
                                      configs['generation']['quantity']['variance'])  # Random variance (-5 to +5)
            modified_quantity = max(1, quantity + variance)  # Ensure quantity is non-negative
            merchandise[product] = modified_quantity


def merchandise_perturbation(merchandise, canAdd, canRemove, probability):
    used_products = list(merchandise.keys())

    if random.random() > probability:
        return merchandise

    if not configs['generation']['products']['canAdd'] and not configs['generation']['products']['canRemove']:
        return merchandise

    if canAdd and random.random() <= probability:
        new_product = random.choice([product for product in products if product not in used_products])
        quantity = random.randint(configs['merchandise']['minimumQuantity'], configs['merchandise']['maximumQuantity'])
        merchandise[new_product] = quantity

        return merchandise

    if canRemove and random.random() <= probability:
        if len(used_products) > 2:
            product_to_remove = random.choice(used_products)
            del merchandise[product_to_remove]

    return merchandise


def modify_route(route):
    used_trips = [trip['from'] for trip in route]
    used_trips.append(route[-1]['to'])

    for node in route:
        merchandise = node["merchandise"]
        merchandise = merchandise_perturbation(merchandise,
                                               configs['generation']['products']['canAdd'],
                                               configs['generation']['products']['canRemove'],
                                               configs['generation']['products']['probability'])
        variate_product_quantities(merchandise)

    if configs['generation']['trips']['canDelete']:
        num_of_deletions = random.randint(0, configs['generation']['trips']['maxDeletions'])

        for _ in range(num_of_deletions):
            deletion_index = random.randint(0, len(route) - 1)
            deletion_trip(route, deletion_index)

    if configs['generation']['trips']['canAppend'] or configs['generation']['trips']['canPrepend'] or \
            configs['generation']['trips']['canInsert']:
        num_extra_nodes = random.randint(0, configs['generation']['trips']['maxAdditions'])

        for _ in range(num_extra_nodes):
            insert_index = random.randint(0, len(route) - 1)

            new_entry_city = random.choice([city for city in cities if city not in used_trips])

            if configs['generation']['trips']['canAppend'] and configs['generation']['trips']['canPrepend'] and \
                    configs['generation']['trips']['canInsert']:
                if insert_index == 0:
                    route.insert(0, prepend_mutation_route(route, new_entry_city))
                elif insert_index == len(route) - 1:
                    route.append(append_mutation_route(route, new_entry_city))
                else:
                    insertion_nodes = insert_mutation_route(route, insert_index, new_entry_city)
                    route.remove(route[insert_index])
                    for index, insertion_node in enumerate(insertion_nodes):
                        route.insert(insert_index + index, insertion_node)

            elif configs['generation']['trips']['canAppend'] and configs['generation']['trips']['canPrepend']:
                if random.choice([True, False]):
                    route.append(append_mutation_route(route, new_entry_city))
                else:
                    route.insert(0, prepend_mutation_route(route, new_entry_city))

            elif configs['generation']['trips']['canAppend']:
                route.append(append_mutation_route(route, new_entry_city))

            elif configs['generation']['trips']['canPrepend']:
                route.insert(0, prepend_mutation_route(route, new_entry_city))

            used_trips.append(new_entry_city)

    return route


def generate_actual_routes(standard_routes, num_routes):
    actual_routes = []
    route_mapping = []
    print(num_routes)
    for actual_route_idx in tqdm(range(num_routes)):
        selected_route = deepcopy(random.choice(standard_routes))
        modified_route = modify_route(selected_route["route"])

        route_mapping.append((actual_route_idx + 1, selected_route['id']))
        actual_routes.append({'id': actual_route_idx + 1, 'route': modified_route})

        if actual_route_idx % (num_routes * 0.10) == 0:
            print(f"{actual_route_idx + int(num_routes * 0.10)} / {num_routes}")
    return actual_routes, route_mapping


def read_configuration(filename):
    with open(os.path.join(os.path.dirname(__file__), filename), 'r') as config_file:
        return json.load(config_file)


def read_standard_routes(filename):
    with open(os.path.join(os.path.dirname(__file__), filename), 'r') as route_file:
        return json.load(route_file)

def format_routes(routes):
    formatted_routes = []

    for i, route in enumerate(routes):
        formatted_route = {'id': i + 1, 'route': route}
        formatted_routes.append(formatted_route)

    return formatted_routes


def save_mapping_to_csv(mappings, output):
    df = pd.DataFrame.from_records(mappings, columns=['actual_route_id', 'standard_route_id'])
    df = df.sort_values(by=['actual_route_id'], ascending=True)
    df.to_csv(os.path.join(os.path.dirname(__file__), output), index=False)


def save_routes_to_json(routes, filename):
    with open(os.path.join(os.path.dirname(__file__), filename), 'w') as json_file:
        json.dump(routes, json_file, indent=3)


configs = None

cities = []
products = []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script for generating standard routes.')
    parser.add_argument('--config', help='Path to the configuration file', required=True)
    parser.add_argument('--input', help='Path to the standard routes file', required=True)
    parser.add_argument('--mapping', help='Path to the mapping file', required=True)
    parser.add_argument('--output', help='Path to the output file', required=True)
    args = parser.parse_args()

    configs = read_configuration(args.config)
    standard_routes = read_standard_routes(args.input)

    cities = configs['trips']['cities']
    products = configs['merchandise']['products']

    num_routes = int(configs["numRoutes"])

    routes, mapping = generate_actual_routes(standard_routes, num_routes)

    print('test')
    save_mapping_to_csv(mapping, args.mapping)
    print('test2')
    save_routes_to_json(routes, args.output)

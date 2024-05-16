import argparse

import random
import json
from copy import deepcopy


# Function to generate a random merchandise
def generate_merchandise():
    merchandise = {}

    used_products = []
    num_products = random.randint(configs['merchandise']['minimumProducts'], configs['merchandise']['maximumProducts'])

    for _ in range(num_products):
        product = random.choice([product for product in products if product not in used_products])
        used_products.append(product)

        quantity = random.randint(configs['merchandise']['minimumQuantity'], configs['merchandise']['maximumQuantity'])
        merchandise[product] = quantity

    return merchandise


# Function to generate a route
def generate_route():
    num_trips = random.randint(configs['trips']['minimum'], configs['trips']['maximum'])

    route = []
    used_trips = []

    from_city = random.choice(cities)

    for _ in range(num_trips):
        to_city = random.choice([city for city in cities if city not in used_trips])

        trip = {'from': from_city, 'to': to_city, 'merchandise': generate_merchandise()}

        route.append(trip)

        used_trips.append(to_city)
        from_city = to_city

    return route


# Function to generate routes
def generate_routes(num_routes):
    generated_routes = []

    for _ in range(num_routes):
        generated_route = generate_route()
        generated_routes.append(generated_route)

    return generated_routes


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


def create_variance_route(variance_route):
    num_trips_to_add = random.randint(2, 4)

    route = variance_route
    used_trips = [trip['from'] for trip in route]
    used_trips.append(route[-1]['to'])

    for trip in route:
        merchandise = trip['merchandise']
        merchandise = merchandise_perturbation(merchandise,
                                               configs['generation']['products']['canAdd'],
                                               configs['generation']['products']['canRemove'],
                                               configs['generation']['products']['probability'])

        if configs['generation']['quantity']['canChange']:
            for product, quantity in merchandise.items():
                if random.random() <= configs['generation']['products']['probability']:
                    variance = random.randint(-configs['generation']['quantity']['variance'],
                                              configs['generation']['quantity']['variance'])
                    modified_quantity = max(1, quantity + variance)
                    merchandise[product] = modified_quantity

    from_city = route[-1]['to']

    for _ in range(num_trips_to_add):
        to_city = random.choice([city for city in cities if city not in used_trips])

        trip = {'from': from_city, 'to': to_city, 'merchandise': generate_merchandise()}

        route.append(trip)

        used_trips.append(to_city)
        from_city = to_city

    return route


def generate_variance_routes(routes, num_routes):
    for route in range(num_routes):
        if random.random() < 0.5:
            generated_route = generate_route()
            routes.append(generated_route)
        else:
            variance_route = random.choice(routes)
            generated_route = create_variance_route(deepcopy(variance_route))
            routes.append(generated_route)

            print(f'generated route \n \t{generated_route} with original route \n \t{ variance_route } \n')

    return routes



def read_configuration(filename):
    with open(filename, 'r') as config_file:
        return json.load(config_file)


def format_routes(routes):
    formatted_routes = []

    for i, route in enumerate(routes):
        formatted_route = {'id': i + 1, 'route': route}
        formatted_routes.append(formatted_route)

    return formatted_routes



def save_routes_to_json(routes, filename):
    with open(filename, 'w') as json_file:
        json.dump(routes, json_file, indent=3)


configs = None

cities = []
products = []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script for generating standard routes.')
    parser.add_argument('--config', help='Path to the configuration file', required=True)
    parser.add_argument('--output', help='Path to the output file', required=True)
    args = parser.parse_args()

    configs = read_configuration(args.config)

    cities = configs['trips']['cities']
    products = configs['merchandise']['products']
    print(configs['overlapping'])
    routes = []

    if configs['overlapping']:
        random_num_routes = int(configs["numRoutes"])
        variance_num_routes = random_num_routes // 2

        routes = generate_routes(variance_num_routes)
        routes = generate_variance_routes(routes, variance_num_routes)
    else:
        routes = generate_routes(configs["numRoutes"])

    save_routes_to_json(format_routes(routes), args.output)

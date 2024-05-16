
import pandas as pd

def calculate_accuracy(pred_df, truth_df, actual_col, standard_col):
    correct = 0
    fails = 0

    for index, row in pred_df.iterrows():
        actual_id = row[actual_col]
        pred_id = row[standard_col]

        correct_id = truth_df[truth_df['actual_route_id'] == actual_id]
        correct_id = correct_id['standard_route_id'].iloc[0]

        if correct_id == pred_id:
            correct += 1
        else:
            fails += 1

    accuracy = correct / (correct + fails)
    return accuracy

truth = pd.read_csv("./data/ground.json")
pred = pd.read_csv('data/route_mapping.csv')
pred2 = pd.read_csv('results_pairwise.csv')

accuracy_pred = calculate_accuracy(pred, truth, 'actual_id', 'standard_id')
accuracy_pred2 = calculate_accuracy(pred2, truth, 'actual_route_id', 'standard_route_id')

print(f"Accuracy for Spark = {accuracy_pred}")
print(f"Accuracy for PairWise = {accuracy_pred2}")
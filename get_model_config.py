import json 

def get_model_config(case_name):
    with open('model_config.json', 'r') as json_file:
        json_dict = json.load(json_file)
        return case_name, json_dict[case_name]

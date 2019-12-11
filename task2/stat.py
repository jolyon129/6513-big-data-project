
# find precision and recall value for semantic label results

# precision = correctly predict / total predict
# recall = correctly predict / total in reality


import pandas as pd
import json
import os

df = pd.ExcelFile('task2_true_label.xlsx').parse('Sheet 1 - task2_label copy') #you could add index_col=0 if there's an index

# total = 0
new_df = df[['Dataset','Label']]
label_map = dict()
for i in range(len(new_df)):
	dataset = new_df.loc[i]['Dataset'].strip('\'')
	f1,f2 = dataset.split('.', maxsplit=1)
	f2 = f2.split('.',maxsplit=1)[0]
	label = new_df.loc[i]['Label']
	if label not in label_map:
		label_map[label] = set()
	label_map[label].add((f1,f2))
	# total +=1

output_files = os.listdir('results')
predicted_label_map = dict()
for file in output_files:
	f1,f2 = file.split('_',maxsplit=1)
	f2 = f2.split('.')[0]

	with open('results/{}'.format(file)) as f:
		data = json.load(f)
		for item in data['columns'][0]['semantic_types']:
			label = item['semantic_type']
			if label not in predicted_label_map:
				predicted_label_map[label] = set()
			predicted_label_map[label].add((f1,f2))
# for item in predicted_label_map.items():
# 	print(item[0], len(item[1]))
# print(len(label_map['street_name']))
# print(len(predicted_label_map['street_name']))
# diff = []
# cnt = 0
# for value in predicted_label_map['street_name']:
# 	print(value)
# 	if value not in label_map['street_name']:
# 		cnt +=1
# 		diff.append(value)
# print(label_map['street_name'])
# print('-------')
# print(diff)
# print(cnt)

# this is total columns of type (number of columns belong to a single type)
total_columns_to_type = sorted([(item[0], len(item[1])) for item in label_map.items()],key=lambda x: x[0])
print(total_columns_to_type)
# for item in label_map.items():
# 	print(item[0], len(item[1]))

# this is total columns predicted as type. (number of columns belong to a single type after prediction)
total_columns_to_type_predicted = sorted([(item[0], len(item[1])) for item in predicted_label_map.items() if item[0] in label_map],key=lambda x: x[0])
print(total_columns_to_type_predicted)

lack = [[(item[0], len(item[1])) for item in predicted_label_map.items() if item[0] not in label_map]]
print(lack)
# for item in predicted_label_map.items():
# 	if item[0] in label_map:
# 		print(item[0], len(item[1]))

# need to computer correctly predicted as type

# print(len(label_map.keys()))
# print(len(predicted_label_map.keys()))
# print(predicted_label_map.keys())



	


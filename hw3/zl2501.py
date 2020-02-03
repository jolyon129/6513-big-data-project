# You need to import everything below
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics

import lime
from lime import lime_text
from lime.lime_text import LimeTextExplainer
from pyspark.sql import Row

import numpy as np

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("hw3") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

########################################################################################################
# Load data
categories = ["alt.atheism", "soc.religion.christian"]
LabeledDocument = pyspark.sql.Row("category", "text")

def categoryFromPath(path):
    return path.split("/")[-2]

def prepareDF(typ):
    rdds = [sc.wholeTextFiles("/user/tbl245/20news-bydate-" + typ + "/" + category)\
              .map(lambda x: LabeledDocument(categoryFromPath(x[0]), x[1]))\
            for category in categories]
    return sc.union(rdds).toDF()

train_df = prepareDF("train").cache()
test_df  = prepareDF("test").cache()

#####################################################################################################
""" Task 1.1
a.	Compute the numbers of documents in training and test datasets. Make sure to write your code here and report
    the numbers in your txt file.
b.	Index each document in each dataset by creating an index column, "id", for each data set, with index starting at 0. 

"""
# Your code starts here
train_size = train_df.count()
test_size = test_df.count()

row_type = Row("category", "text", "id")
test_df = spark.createDataFrame(
    test_df.rdd.zipWithIndex().map(lambda x: row_type(x[0].category, x[0].text, x[1])))
train_df = spark.createDataFrame(
    train_df.rdd.zipWithIndex().map(lambda x: row_type(x[0].category, x[0].text, x[1])))

test_df.show(5,truncate=True)

########################################################################################################
# Build pipeline and run
indexer   = StringIndexer(inputCol="category", outputCol="label")
tokenizer = RegexTokenizer(pattern=u'\W+', inputCol="text", outputCol="words", toLowercase=False)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf       = IDF(inputCol="rawFeatures", outputCol="features")
lr        = LogisticRegression(maxIter=20, regParam=0.001)

# Builing model pipeline
pipeline = Pipeline(stages=[indexer, tokenizer, hashingTF, idf, lr])

# Train model on training set
model = pipeline.fit(train_df)   #if you give new names to your indexed datasets, make sure to make adjustments here

# Model prediction on test set
pred = model.transform(test_df)  # ...and here

# Model prediction accuracy (F1-score)
pl = pred.select("label", "prediction").rdd.cache()
metrics = MulticlassMetrics(pl)
metrics.fMeasure()

#####################################################################################################
""" Task 1.2
a.	Run the model provided above. 
    Take your time to carefully understanding what is happening in this model pipeline.
    You are NOT allowed to make changes to this model's configurations.
    Compute and report the F1-score on the test dataset.
b.	Get and report the schema (column names and data types) of the model's prediction output.

"""
# Your code for this part, IF ANY, starts here

print(pred.columns)


#######################################################################################################
#Use LIME to explain example
class_names = ['Atheism', 'Christian']
explainer = LimeTextExplainer(class_names=class_names)

# Choose a random text in test set, change seed for randomness 
test_point = test_df.sample(False, 0.1, seed = 10).limit(1)
test_point_label = test_point.select("category").collect()[0][0]
test_point_text = test_point.select("text").collect()[0][0]

def classifier_fn(data):
    spark_object = spark.createDataFrame(data, "string").toDF("text")
    pred = model.transform(spark_object)   #if you build the model with a different name, make appropriate changes here
    output = np.array((pred.select("probability").collect())).reshape(len(data),2)
    return output

exp = explainer.explain_instance(test_point_text, classifier_fn, num_features=6)
print('Probability(Christian) =', classifier_fn([test_point_text])[0][1])
print('True class: %s' % class_names[categories.index(test_point_label)])
exp.as_list()

#####################################################################################################
""" 
Task 1.3 : Output and report required details on test documents with ID’s 0, 275, and 664.
Task 1.4 : Generate explanations for all misclassified documents in the test set, sorted by conf in descending order, 
           and save this output (index, confidence, and LIME's explanation) to netID_misclassified_ordered.csv for submission.
"""
# Your code starts here

selected_rows = pred.filter("id==0 or id==275 or id==664")
for row in selected_rows.collect():
    exp = explainer.explain_instance(row.text, classifier_fn, num_features=6)
    print(f'id:{row.id}, category:{row.category}, probabilities:{row.probability},prediction:{row.prediction}, LIMES:{exp.as_list()}')

# find the misclassified
misclassifed = pred.rdd.filter(lambda x: x.label!=x.prediction)
misclassifed_collected = misclassifed.rdd.collect()
# Add conf
from pyspark.sql import Row
row_type = Row(*pred.columns, 'conf')


def any_f(x: Row, row_type=row_type):
    conf = abs(float(x.probability[0] - x.probability[1]))
    return row_type(x.category, x.text, x.id, x.label, x.words, x.rawFeatures, x.features,
                    x.rawPrediction, x.probability, x.prediction, conf)


misclassifed = spark.createDataFrame(misclassifed.map(any_f))

result = []
count = 0
for row in misclassifed_collected:
    exp = explainer.explain_instance(row.text, classifier_fn, num_features=6).as_list()
    result.append((row.id, row.conf, exp))
    count += 1
    print(f'{count} finished.')

result = sorted(result, key=lambda x:x[1],reverse=True)
result_rdd = sc.parallelize(result)
result_rdd.saveAsTextFile("zl2501_misclassified_ordered.out")



########################################################################################################
""" Task 1.5
Get the word and summation weight and frequency
"""
# Your code starts here
words_rdd = result_rdd.flatMap(lambda x: x[2])
words_weight = words_rdd.groupByKey().mapValues(lambda x: (len([w for w in x]),sum([abs(w) for w in x])))
words_weight = words_weight.map(lambda x: (x[0],x[1][0],x[1][1]))
words_weight = words_weight.sortBy(lambda x:x[1],ascending=False)
words_weight.saveAsTextFile("zl2501_words_weight.out")

########################################################################################################
""" Task 2
Identify a feature-selection strategy to improve the model's F1-score.
Codes for your strategy is required
Retrain pipeline with your new train set (name it, new_train_df)
You are NOT allowed make changes to the test set
Give the new F1-score.
"""

#Your code starts here

banned_words = ['Alan','Asimov','Carl','Julian','Olsen','Todd','Stanley','Peter']
def f1(row:Row, banned_words=banned_words):
    row_type = Row("category","text","id")
    text = row.text
    for word in banned_words:
        text = text.replace(word,'')
    return row_type(row.category,text,row.id)

# Train on new data
new_train_df = spark.createDataFrame(train_df.rdd.map(f1)).cache()

# Test new model
new_model = pipeline.fit(new_train_df)
new_pred = new_model.transform(test_df)
new_pl = new_pred.select("label", "prediction").rdd.cache()
new_metrics = MulticlassMetrics(new_pl)
new_metrics.fMeasure()

targets = result_rdd.filter(lambda x:x[1]>=0.1)
targets_id = targets.map(lambda x: x[0]).collect()
target_pred = new_pred.rdd.filter(lambda x: x.id in targets_id and x.prediction==x.label)
mis = spark.createDataFrame(target_pred.map(any_f))
new_result = []
count = 0
for row in mis.collect():
    exp = explainer.explain_instance(row.text, classifier_fn, num_features=6).as_list()
    new_result.append((row.id, row.conf, exp))
    count += 1
    print(f'{count} finished.')

print(new_result)








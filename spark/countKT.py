from pyspark import SparkContext
logFile = "file:///home/jovyan/work/trial.txt"
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numKs = logData.filter(lambda s: 'K.' in s).count()
numTs = logData.filter(lambda s: 'trial' in s).count()
print("Lines with K: %i, lines with trial: %i" % (numKs, numTs))
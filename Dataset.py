import json
import pandas as pd
import os
from collections import Counter
import lsh
from lsh.cache import Cache
from lsh.minhash import MinHasher
import hashlib
from random import shuffle
class Dataset:
    def __init__(self, path_to_data=None, input_type=None, save_data_as_json=None, data_field=None, target_field=None, data_column=1, target_column=2, name=None, deduplicate=False, hasTitle=False, labels=None):
        """This class will abstract a csv, tsv, or json dataset to make it usable as an abstract list

        :param path_to_data: a path to a dataset (This can be a list of files as well)
        :param input_type: if there is not explicit extension on your file (i.e. .csv) this needs to be specified
        :param save_data_as_json: flag to indicate whether you want to save out the usable dataset
        :param data_field: a list indicating how to get to the data field of your json
        :param target_field: a list indicating how to get to the target field of your json
        :param data_column: the column of the tsv or csv your data is in (starting with column 0)
        :param target_column: the column of the tsv or csv your target is in (starting with column 0)
        :param name: the name of a previously loaded dataset if you saved it (all datasets are saved under ./Datasets/.datasets)
        :param deduplicate: a flag to configure if the data is deduplicated or not default is false
        :param hasTitle: a flag to indicate if you csv/tsv has a tile row
        :param labels: A list of 2 labels to use from csv or tsv file to define the columns

        """
        # If a dataset name is given, check if it's one of the saved datasets
        self.nameDict = dict()
        if os.path.exists("./Datasets/.datasets"):

            with open("./Datasets/.datasets", "r") as datas:
                content = datas.readlines()

                if not content == []:
                    nameList = content

                    for line in nameList:
                        line = line.split(" ")
                        self.nameDict[line[0]] = line[1]

                    if name in self.nameDict.keys():
                        self.path = self.nameDict[name]

                    if name not in self.nameDict.keys() and name is not None and path_to_data is None:
                        raise ValueError("That is not a known dataset.")
        else:
            with open("./Datasets/.datasets", "w+") as out:
                out.write("")

        # Setup fields for class
        if name is None or name not in self.nameDict.keys():
            self.path = path_to_data

        if os.path.isdir(self.path):
            self.files = os.listdir(self.path)

        else:
            self.files = [""]

        self.input_type = input_type

        if not input_type and not os.path.isdir(self.path):
            self.input_type = self.path.split(".")[-1]

        if self.input_type not in ("csv", "tsv", "json", "jsonl"):
            raise ValueError("This is not a supported data type.")

        self.save_data = save_data_as_json
        self.data_drill = data_field
        self.targetDrill = target_field
        self.data_column = data_column
        self.target_column = target_column
        self.temp_data= []
        self.data = []
        self.target = []
        self.dataitems = []
        self.hasTitle = hasTitle
        self.labels=labels

        if name is None:
            self.name = self.path.split("/")[-1].split(".")[0]

        else:
            self.name = name


        # Actual object init
        if self.name in self.nameDict.keys():

            self.path = self.nameDict[self.name]
            self.quickLoad()

        else:

            if len(self.files) > 1:
                for f in self.files:
                    self.loadData(self.path + "/" + f)
            else:
                self.loadData(self.path)
            if deduplicate:
                dups = self.dedup()
                dupset = set()
                templist = []

                for item in dups:
                    dupset.add(item[1])

                for x, item in enumerate(self.data):

                    if x not in dupset:
                        templist.append(item)

                self.data = templist

            self.saveData()

        self.classes= set(self.target)


    def loadData(self, path):
        print("Loading dataset...")
        with open(path, "r") as f:
            print(self.input_type)

            if self.input_type in ("json", "jsonl"):
                lines = f.readlines()

                for line in lines:
                    self.temp_data.append(json.loads(line))

                return self.jsonFormat(self.temp_data)

            elif self.input_type == "csv":
                raw = pd.read_csv(path)
                return self.csvToJson(raw, self.labels)

            elif self.input_type == "tsv":
                raw = pd.read_csv(path, delimiter='\t')
                return self.csvToJson(raw, self.labels)


    def saveData(self):
        if self.name is not None and self.name not in self.nameDict.keys():
            if not os.path.exists("./Datasets"):
                os.mkdir("./Datasets")

            path = "./Datasets/" + self.name + "." + "jsonl" 
            with open(path, "a+") as f:
                for entry in self.data:
                    formatter = dict()
                    formatter["data"] = entry[0]
                    formatter["target"] = entry[1]
                    f.write(json.dumps(formatter) + "\n")

            with open("./Datasets/.datasets", "a+") as f:
                f.write(self.name + " " + path + " \n")

        else:
            if not os.path.exists("./Datasets"):
                os.mkdir("./Datasets")

            path = "./Datasets/" + self.name + "." + "jsonl"
            with open(path, "a+") as f:
                for entry in self.data:
                    formatter = dict()
                    formatter["data"] = entry[0]
                    formatter["target"] = entry[1]
                    f.write(json.dumps(formatter) + "\n")

            with open("./Datasets/.datasets", "a+") as f:
                f.write( self.name + " " + path + " \n")

    def shuffle(self):
        shuffle(self.data)


    def jsonFormat(self, raw):
        for line in raw:
            data = line
            for field in self.data_drill:
                data = data[field]

            self.dataitems.append(data)
            sent = line

            for field in self.targetDrill:
                sent = sent[field]

            self.target.append(sent)
            self.data.append((data, sent))


    def csvToJson(self, raw, labels=None):
        """ Sets up data abstraction from a pandas dataframe

        :param raw: raw data (pandas dataframe)
        :param labels: a list of labels (only 2) that define the columns 

        """
        if self.hasTitle:
            z = 1

        else:
            z = 0

        if labels:
            for x in zip(raw[labels[0]], raw[labels[1]]):
                self.data.append(x)

        else:
            for x in zip(raw.iloc[z:,self.data_column], raw.iloc[z:,self.target_column]):
                self.data.append(x)

        self.dataitems = raw[labels[0]]
        self.target = raw[labels[1]]


    def dataStats(self):
        counts = Counter(self.target)
        print("""Data Stats:
        Dataset Length: {}
        Max Data Length: {} Chars {} Words
        """.format(len(self.data), max([len(x[0]) for x in self.data]), max([len(x) for x in [y[0].split(" ") for y in self.data]])))

        for _class in self.classes:
            print("Percentage {}: {}".format(_class, counts[_class]/len(self.target)))

    def encodeClasses(self, defDict=None):
        """ This function will encode the classes. Just pass in a dictionary with the class name and encode or let it default to 0-x where x is num of classes

        :param defDict: a dictionary defining class labels
        :returns: None
        """
        if defDict is None:
            classes = { x: y for y, x in enumerate(Counter(self.target).keys()) }

        else:
            classes = defDict

        templist=[]

        for item in self.target:

            try:
                templist.append(classes[item])

            except Exception as e:
                print("Unkown Class encountered")
                continue

        self.target = templist
        self.data = zip(self.dataitems, self.target)

    def split(self, split, shuffle=False):
        """ Splits the data into train/test

        :param split: a percentage to split by (e.g. 0.80)
        :param shuffle: a flag to shuffle data before splitting
        :returns: a tuple (train, test)
        :rtype: tuple

        """
        splitter = int((len(self.data) -1) * split)
        return (self.data[:splitter], self.data[splitter:])


    def dedup(self):
        deduper = Cache(MinHasher(100))
        for x, doc in enumerate(self.data):
            deduper.add_doc(doc[0], x)
        dups = deduper.get_all_duplicates(min_jaccard=0.80)
        return dups


    def quickLoad(self):
        print("Dataset has been processed in the past. Quickloading...")

        with open("./Datasets/{}.jsonl".format(self.name), "r") as f:
            lines = f.readlines()

            for line in lines:
                self.temp_data.append(json.loads(line))

            for entry in self.temp_data:
                self.data.append((entry["data"], entry["target"]))
                self.target.append(entry["target"])
                self.dataitems.append(entry["data"])


    def __len__():
        return len(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value
        
# The main method demonstrates a use of this 
def __main__():
     a = Dataset(path_to_data="/Users/vlurgio/TwitterData/data/newlyHydrated/shorttweets.jsonl", data_field=["text"], target_field=["id_str"], input_type="jsonl")
     a.dataStats()
if __name__ == "__main__":
    __main__()



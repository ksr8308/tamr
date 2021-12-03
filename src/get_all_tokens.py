#!/bin/python
import os
import glob
import csv
import argparse
import re
from chardet.universaldetector import UniversalDetector
from os.path import basename


def tokenize_key(phrase):
    result = phrase.replace('_', ' ')
    regex_number = re.compile(r'[0-9]+', re.IGNORECASE)
    result = regex_number.sub(' ', result)
    return result.split()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-folder", dest="inputfolder", type=str, default=None,
                        help="absolute path to input directory")
    args = parser.parse_args()

    path_of_src = os.path.dirname(os.path.realpath(__file__))
    path_of_data = os.path.abspath(path_of_src + "/../data/")
    path_of_output = os.path.abspath(path_of_src + "/../output/")

    if args.inputfolder is None:
        print("INFO ==> No input folder has been specified")
        print("INFO ==> Will use the default data/ folder: {}".format(path_of_data))
        input_folder = path_of_data
    else:
        if os.path.exists(args.inputfolder):
            input_folder = args.inputfolder
        else:
            print("ERROR ==> Specified input folder {} doesn't exist".format(args.inputfolder))
            exit(0)

    input_files = glob.glob(input_folder + '/*.csv')
    if len(input_files) == 0:
        print("ERROR ==> No csv files can be found in the input folder {}".format(input_folder))
        exit(0)

    dict_tokens = {}
    detector = UniversalDetector()

    for input_file in input_files:
        file_name = basename(input_file)
        print("INFO ==> Processing {}".format(file_name))
        detector.reset()

        for line in open(input_file, 'rb'):
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        encoding = detector.result

        with open(input_file, 'r', encoding=encoding['encoding']) as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',', quotechar='"', escapechar='\\')
            for row in reader:
                for key in row.keys():
                    tokens = tokenize_key(key)
                    for token in tokens:
                        if token not in dict_tokens:
                            dict_tokens[token] = []
                            dict_tokens[token].append(file_name + ' | ' + key)
                        else:
                            dict_tokens[token].append(file_name + ' | ' + key)
                break

    with open(path_of_output + '/token_dict.csv', 'w', encoding='utf-8') as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', escapechar='\\', quoting=csv.QUOTE_ALL)
        writer.writerow(["token", "full_words", "source_columns"])
        for token in sorted(dict_tokens.keys()):
            writer.writerow([token, "", dict_tokens[token][:5]])

    print("INFO ==> All tokens have been saved into file {}".format(path_of_output + "/token_dict.csv"))

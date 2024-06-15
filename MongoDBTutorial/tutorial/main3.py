from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
from datetime import datetime as dt
load_dotenv(find_dotenv())

import json

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://muradDB96:{password}@atlascluster.qu0yows.mongodb.net/?retryWrites=true&w=majority&authSource=admin"

client = MongoClient(connection_string)

dbs = client.list_database_names()

printer = pprint.PrettyPrinter()

jeoprady_db = client.jeoprady_db
question = jeoprady_db.question


# with open(r'C:\Users\MuradKulbuzhev\Desktop\Tutorials\MongoDBTutorial\tutorial\JEOPARDY_QUESTIONS1(1).json') as file:
#     daten = json.load(file)


# if isinstance(daten, list):
#     # Falls die JSON-Datei eine Liste von Dokumenten enthält
#     question_collection.insert_many(daten)
# else:
#     # Falls die JSON-Datei ein einzelnes Dokument enthält
#     question_collection.insert_one(daten)


def fuzzy_matching():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "text": {
                    "query": "computer",
                    "path": "category",
                    # "fuzzy": {}
                    "synonyms": "mapping"
                }
            }
        }

    ])

    printer.pprint(list(result))

def autocomplete():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "autocomplete": {
                    "query": "computer programmer",
                    "path": "question",
                    "tokenOrder": "sequential",
                    "fuzzy": {}
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "question": 1

            }
        }

    ])

    printer.pprint(list(result))

def compound_queries():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": ["COMPUTER", "CODING"],
                                "path": "category"
                            }
                        }
                    ],
                    "mustNot": [{
                        "text": {
                            "query": "codes",
                            "path": "category"
                        }
                    }],
                    "should": [
                        {
                            "text": {
                                "query": "application",
                                "path": "answer"
                            }
                        }
                    ]
                }
            }
        },
        {
            "$project": {
                "question": 1,
                "answer": 1,
                "category": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ])

    printer.pprint(list(result))

def relevance():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": "geography",
                                "path": "category"
                            }

                        },

                    ],
                    "should": [
                        {
                            "text": {
                                "query": "Final Jeopardy",
                                "path": "round",
                                "score": {"boost": {"value": 3.0}}
                            }
                        },
                        {
                            "text": {
                                "query": "Double Jeopardy",
                                "path": "round",
                                "score": {"boost": {"value": 2.0}}
                            }
                        }
                    ]
                }
            } 
        }, {
            "$project": {
                "question": 1,
                "answer": 1,
                "category": 1,
                "round": 1,
                "score": {"$meta": "searchScore"}
            }
        },
        {
            "$limit": 10
        }
    ])

    printer.pprint(list(result))

relevance()
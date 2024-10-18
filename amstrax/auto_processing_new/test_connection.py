print("Hello, world!")

import sys

print(sys.path)

import amstrax


rundb = amstrax.get_mongo_collection()

print(rundb.find_one())

print("Goodbye, world!")

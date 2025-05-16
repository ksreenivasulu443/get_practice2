from pymongo import MongoClient

# Paste your MongoDB connection string here
client = MongoClient("mongodb://localhost:27017")

# Choose the database and collection (replace with your names)
db = client["mydb"]
collection = db["Contact_Info"]

# Example: find documents where Surname is "Arun"
results = collection.find({"Surname": "Arun"})

for doc in results:
    print(doc)

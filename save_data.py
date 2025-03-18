import pymongo

# MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["local"]  # Replace with your database name
collection = db["jobs"]  # Replace with your collection name

# Fetch all data from the collection
data = list(collection.find())

if data:
    # Extract field names from the first document
    fieldnames = data[0].keys()
    
    # Save data to CSV
    with open("output.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print("Data saved to output.csv")
else:
    print("No data found in the collection.")

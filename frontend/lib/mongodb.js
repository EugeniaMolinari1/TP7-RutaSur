
import { MongoClient } from "mongodb";

const uri = process.env.MONGODB_URI || "mongodb://localhost:27017/?replicaSet=rs0";

const options = {};

let clientPromise;

if (!globalThis._mongoClientPromise) {
    const client = new MongoClient(uri, options);
    globalThis._mongoClientPromise = client.connect();
}

clientPromise = globalThis._mongoClientPromise;

export async function getDatabase() {
    const client = await clientPromise;
    return client.db("rutasur");
}

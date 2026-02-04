const { MongoClient } = require('mongodb');

let client;
let db;

async function connectDB() {
  client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  db = client.db('afonsystem');
  console.log('Connected to MongoDB (afonsystem)');
}

function getDB() {
  if (!db) throw new Error('Database not connected');
  return db;
}

async function closeDB() {
  if (client) await client.close();
}

module.exports = { connectDB, getDB, closeDB };

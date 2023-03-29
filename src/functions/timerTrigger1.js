const { app } = require('@azure/functions');
const { BlobServiceClient } = require('@azure/storage-blob');
const MongoClient = require('mongodb').MongoClient;
const atob = require('atob')
require('dotenv').config();

const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.BLOB_CONNECTION_STRING);
const containerName = process.env.CONTAINER_NAME;

const mongoUrl = process.env.MONGO_CONNECTION_STRING;
const mongoDbName = 'AverageTemperature';
app.timer('timerTrigger1', {
    schedule: '0 0 * * * *',
    async handler(myTimer, context) {
        const now = new Date();
        const containerClient = blobServiceClient.getContainerClient(containerName);
        const hourDirectory = `${now.getUTCFullYear()}/${(now.getUTCMonth() + 1).toString().padStart(2, '0')}/${now.getUTCDate().toString().padStart(2, '0')}/${(now.getUTCHours() - 1).toString().padStart(2, '0')}`;
        const blobsInHour = containerClient.listBlobsFlat({ prefix: `IoTKurssi/00/${hourDirectory}/`, includeMetadata: true });

        let dailyTemperatures = [];
        let dailyTimestamps = [];
        for await (const blob of blobsInHour) {

            const blobClient = containerClient.getBlobClient(blob.name);
            const downloadBlockBlobResponse = await blobClient.download(0);
            const data = await streamToString(downloadBlockBlobResponse.readableStreamBody);
            const firstLine = data.split('\n')[0];
            const body = JSON.parse(atob(JSON.parse(firstLine).Body));
            const { Timestamp, Temperature } = body;
            dailyTemperatures.push({ temperature: parseFloat(Temperature) });
            dailyTimestamps.push(new Date(Timestamp).getTime());
        }
        const dailyAverage = dailyTemperatures.reduce((acc, curr) => acc + curr.temperature, 0) / dailyTemperatures.length;

        const timestampIndex = 0; // Replace with the index of the timestamp you want to modify
        const timestamp = new Date(dailyTimestamps[timestampIndex]);
        timestamp.setMinutes(0);
        timestamp.setSeconds(0);
        
        let hours = Math.floor((timestamp / 1000 / 60 / 60) % 24);
        hours += 4;

        const Data = {
            timestamp: hours,
            HourAverage: dailyAverage
        };
        const client = new MongoClient(mongoUrl, { useUnifiedTopology: true });
        await client.connect();

        let date = new Date();
        date.setUTCHours(date.getUTCHours() + 2);
        const today = date.toISOString().slice(0, 10);
        const collectionName = `${today}`;
        const db = client.db(mongoDbName);
        let collection = db.collection(collectionName);
        //const dailyAverageDoc = await collection.findOne({ date: today });
        const collectionExists = await db.listCollections({ name: collectionName }).hasNext();

        if (collectionExists) {
            const result = await collection.insertOne(Data);
        } else {
            //date = dailyTemperatures[0].timestamp;
            await db.createCollection(collectionName);
            collection = db.collection(collectionName);
            const result = await collection.insertOne(Data);
        }

        // Insert or update dailyAverageDoc in the collection
        //await collection.updateOne({ date: today }, { $set: { dailyAverage: dailyAverage } }, { upsert: true });
        //await collection.insertOne(data);
        await client.close();

    }
});

const streamToString = async (readableStream) => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        readableStream.on("data", (data) => {
            chunks.push(data.toString());
        });
        readableStream.on("end", () => {
            resolve(chunks.join(""));
        });
        readableStream.on("error", reject);
    });
};
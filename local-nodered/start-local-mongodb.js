const { MongoMemoryServer } = require("mongodb-memory-server");

async function main() {
    const mongod = await MongoMemoryServer.create({
        instance: {
            port: 27017,
            ip: "127.0.0.1",
            dbName: "smartmobility",
        },
    });

    console.log("MongoDB memory server started");
    console.log(`URI=${mongod.getUri()}`);

    const shutdown = async () => {
        try {
            await mongod.stop();
        } finally {
            process.exit(0);
        }
    };

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
}

main().catch((error) => {
    console.error("Failed to start MongoDB memory server");
    console.error(error);
    process.exit(1);
});

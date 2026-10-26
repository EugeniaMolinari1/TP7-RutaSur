
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const kafka = new Kafka({
    clientId: "rutasur-analitica-replay",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "analitica-tiempo-hub",
});

const mongo = new MongoClient(
    process.env.MONGO_URL ||
    "mongodb://localhost:27017/?directConnection=true"
);

async function iniciar() {
    await mongo.connect();

    const db = mongo.db("rutasur");
    const ingresos = db.collection("analitica_replay_ingresos");
    const tiempos = db.collection("analitica_tiempos_hub");

    await ingresos.createIndex(
        { shipmentId: 1 },
        { unique: true }
    );

    await tiempos.createIndex(
        { hubId: 1 },
        { unique: true }
    );

    await consumer.connect();

    await consumer.subscribe({
        topic: "envios.estados",
        fromBeginning: true,
    });

    console.log("Reprocesamiento histórico iniciado");

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({ topic, partition, message }) => {
            const evento = JSON.parse(message.value.toString());

            if (evento.tipo === "INGRESADO_HUB") {
                await ingresos.updateOne(
                    { shipmentId: evento.shipmentId },
                    {
                        $setOnInsert: {
                            shipmentId: evento.shipmentId,
                            hubId: evento.hubId,
                            ocurridoEn: evento.ocurridoEn,
                        },
                    },
                    { upsert: true }
                );
            }

            if (evento.tipo === "EN_REPARTO") {
                const ingreso = await ingresos.findOne({
                    shipmentId: evento.shipmentId,
                });

                if (ingreso) {
                    const minutos =
                        (new Date(evento.ocurridoEn) -
                            new Date(ingreso.ocurridoEn)) / 60000;

                    if (minutos >= 0) {
                        const registrado = await db
                            .collection("analitica_replay_procesados")
                            .findOne({ eventId: evento.eventId });

                        if (!registrado) {
                            await tiempos.updateOne(
                                { hubId: ingreso.hubId },
                                {
                                    $inc: {
                                        totalMinutos: minutos,
                                        cantidad: 1,
                                    },
                                },
                                { upsert: true }
                            );

                            await db
                                .collection("analitica_replay_procesados")
                                .updateOne(
                                    { eventId: evento.eventId },
                                    {
                                        $setOnInsert: {
                                            eventId: evento.eventId,
                                        }
                                    },
                                    { upsert: true }
                                );
                        }
                    }
                }
            }

            await consumer.commitOffsets([{
                topic,
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
            }]);
        },
    });
}

iniciar().catch(console.error);


const crypto = require("node:crypto");
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const kafka = new Kafka({
    clientId: "rutasur-notificaciones",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "notificaciones",
});

const producer = kafka.producer();

const mongo = new MongoClient(
    process.env.MONGO_URL ||
    "mongodb://localhost:27017/?directConnection=true"
);

async function iniciar() {
    await mongo.connect();

    const procesados = mongo
        .db("rutasur")
        .collection("eventos_procesados");

    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({
        topics: ["envios.estados", "alertas.sla"],
        fromBeginning: true,
    });

    console.log("Servicio Notificaciones iniciado");

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({ topic, partition, message }) => {
            const evento = JSON.parse(message.value.toString());

            console.log(
                `eventId=${evento.eventId} particion=${partition} offset=${message.offset}`
            );

            const corresponde =
                (topic === "envios.estados" &&
                    evento.tipo === "ENTREGA_FALLIDA") ||
                (topic === "alertas.sla" &&
                    evento.estado !== "CERRADA");

            if (corresponde) {
                const idNotificacion =
                    `notificacion:${evento.eventId}`;

                const existente = await procesados.findOne({
                    consumidor: "notificaciones",
                    eventId: evento.eventId,
                });

                if (!existente) {
                    const notificacion = {
                        eventId: idNotificacion,
                        shipmentId: evento.shipmentId,
                        tipo: "NOTIFICACION_ENVIADA",
                        ocurridoEn: evento.ocurridoEn,
                        secuencia: evento.secuencia,
                        hubId: evento.hubId,
                        zonaId: evento.zonaId,
                        repartidorId: evento.repartidorId ?? null,
                        version: 1,
                        motivo: evento.motivo ?? evento.regla,
                        mensaje:
                            "Aviso al destinatario sobre el estado del envío",
                    };

                    await producer.send({
                        topic: "notificaciones.enviadas",
                        messages: [{
                            key: evento.shipmentId,
                            value: JSON.stringify(notificacion),
                        }],
                    });

                    await procesados.updateOne(
                        {
                            consumidor: "notificaciones",
                            eventId: evento.eventId,
                        },
                        {
                            $setOnInsert: {
                                consumidor: "notificaciones",
                                eventId: evento.eventId,
                                procesadoEn: new Date(),
                            },
                        },
                        { upsert: true }
                    );

                    console.log(
                        `Notificación publicada: ${evento.shipmentId}`
                    );
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

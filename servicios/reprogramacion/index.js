
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const kafka = new Kafka({
    clientId: "rutasur-reprogramacion",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "reprogramacion",
});

const producer = kafka.producer();

const mongo = new MongoClient(
    process.env.MONGO_URL ||
    "mongodb://localhost:27017/?directConnection=true"
);

function siguienteDiaHabil(fecha) {
    const siguiente = new Date(fecha);
    siguiente.setUTCDate(siguiente.getUTCDate() + 1);

    while ([0, 6].includes(siguiente.getUTCDay())) {
        siguiente.setUTCDate(siguiente.getUTCDate() + 1);
    }

    return siguiente.toISOString();
}

function decidirReprogramacion(evento) {
    if (evento.intento >= 3) {
        return { decision: "DEVOLUCION_COMERCIO" };
    }

    switch (evento.motivo) {
        case "AUSENTE":
            return {
                decision: "REPROGRAMADO",
                proximoIntento: evento.intento + 1,
                fecha: siguienteDiaHabil(evento.ocurridoEn),
                franja:
                    evento.franja === "MANANA"
                        ? "TARDE"
                        : "MANANA",
            };

        case "DIRECCION_INCORRECTA":
            return { decision: "RETENIDO" };

        case "RECHAZADO":
            return { decision: "DEVOLUCION_COMERCIO" };

        case "ZONA_INACCESIBLE":
            return { decision: "RETENIDO" };

        default:
            throw new Error("Motivo de fallo desconocido");
    }
}

async function iniciar() {
    await mongo.connect();

    const procesados = mongo
        .db("rutasur")
        .collection("eventos_procesados");

    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({
        topic: "envios.estados",
        fromBeginning: true,
    });

    console.log("Servicio Reprogramación iniciado");

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({ topic, partition, message }) => {
            const evento = JSON.parse(message.value.toString());

            console.log(
                `eventId=${evento.eventId} particion=${partition} offset=${message.offset}`
            );

            if (evento.tipo === "ENTREGA_FALLIDA") {
                const existente = await procesados.findOne({
                    consumidor: "reprogramacion",
                    eventId: evento.eventId,
                });

                if (!existente) {
                    const decision = decidirReprogramacion(evento);

                    const resultado = {
                        eventId: `reprogramacion:${evento.eventId}`,
                        shipmentId: evento.shipmentId,
                        tipo: "REPROGRAMACION_DECIDIDA",
                        ocurridoEn: evento.ocurridoEn,
                        secuencia: evento.secuencia,
                        hubId: evento.hubId,
                        zonaId: evento.zonaId,
                        repartidorId: evento.repartidorId ?? null,
                        version: 1,
                        ...decision,
                    };

                    await producer.send({
                        topic: "envios.reprogramaciones",
                        messages: [{
                            key: evento.shipmentId,
                            value: JSON.stringify(resultado),
                        }],
                    });

                    await procesados.updateOne(
                        {
                            consumidor: "reprogramacion",
                            eventId: evento.eventId,
                        },
                        {
                            $setOnInsert: {
                                consumidor: "reprogramacion",
                                eventId: evento.eventId,
                                procesadoEn: new Date(),
                            },
                        },
                        { upsert: true }
                    );

                    console.log(
                        `${evento.shipmentId}: ${decision.decision}`
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

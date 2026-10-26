
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");
const { validarEvento } = require("./src/validarEvento");

const kafka = new Kafka({
    clientId: "rutasur-proyector",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "rutasur-proyector",
});

const producer = kafka.producer();

const mongo = new MongoClient(
    process.env.MONGO_URL ||
    "mongodb://localhost:27017/?directConnection=true"
);

const TOPICS = [
    "envios.estados",
    "envios.reprogramaciones",
    "alertas.sla",
    "notificaciones.enviadas",
];

async function enviarDLQ(topic, partition, message, errores) {
    await producer.send({
        topic: "envios.estados.dlq",
        messages: [{
            key: message.key?.toString() || undefined,
            value: JSON.stringify({
                eventoOriginal: message.value?.toString() || null,
                errores,
                topicOriginal: topic,
                particionOriginal: partition,
                offsetOriginal: message.offset,
            }),
        }],
    });
}

async function procesarEvento(db, topic, evento) {
    const procesados = db.collection("eventos_procesados");
    const envios = db.collection("envios");

    const session = mongo.startSession();

    try {
        await session.withTransaction(async () => {
            const registrado = await procesados.findOne({
                consumidor: "proyector",
                eventId: evento.eventId,
            }, { session });

            if (registrado) {
                console.log(`Duplicado ignorado: ${evento.eventId}`);
                return;
            }

            if (topic === "envios.estados") {
                const actual = await envios.findOne(
                    { shipmentId: evento.shipmentId },
                    { session }
                );

                const ultimaSecuencia = actual?.ultimaSecuencia ?? 0;

                if (evento.secuencia > ultimaSecuencia) {
                    const historial = {
                        eventId: evento.eventId,
                        tipo: evento.tipo,
                        ocurridoEn: evento.ocurridoEn,
                        secuencia: evento.secuencia,
                    };

                    if (evento.tipo === "ENTREGA_FALLIDA") {
                        historial.motivo = evento.motivo;
                        historial.intento = evento.intento;
                        historial.franja = evento.franja;
                    }

                    const cambios = {
                        shipmentId: evento.shipmentId,
                        estado: evento.tipo,
                        hubId: evento.hubId,
                        zonaId: evento.zonaId,
                        repartidorId: evento.repartidorId,
                        ultimaSecuencia: evento.secuencia,
                        actualizadoEn: new Date(evento.ocurridoEn),
                    };

                    const actualizacion = {
                        $set: cambios,
                        $push: {
                            historial: {
                                $each: [historial],
                                $slice: -15,
                            },
                        },
                    };

                    if (evento.tipo === "ENTREGA_FALLIDA") {
                        actualizacion.$push.intentos = {
                            $each: [{
                                intento: evento.intento,
                                motivo: evento.motivo,
                                franja: evento.franja,
                                ocurridoEn: evento.ocurridoEn,
                            }],
                            $slice: -3,
                        };
                    }

                    await envios.updateOne(
                        { shipmentId: evento.shipmentId },
                        actualizacion,
                        { upsert: true, session }
                    );

                    console.log(
                        `Estado aplicado: ${evento.shipmentId} -> ${evento.tipo}`
                    );
                } else {
                    console.log(
                        `Secuencia antigua ignorada: ${evento.shipmentId}`
                    );
                }
            }

            if (topic === "envios.reprogramaciones") {
                await envios.updateOne(
                    { shipmentId: evento.shipmentId },
                    {
                        $set: {
                            reprogramacion: evento,
                            actualizadoEn: new Date(),
                        },
                    },
                    { upsert: true, session }
                );
            }

            if (topic === "alertas.sla") {
                await db.collection("alertas").updateOne(
                    {
                        shipmentId: evento.shipmentId,
                        regla: evento.regla,
                    },
                    { $set: evento },
                    { upsert: true, session }
                );
            }

            if (topic === "notificaciones.enviadas") {
                await db.collection("notificaciones").updateOne(
                    { eventId: evento.eventId },
                    { $setOnInsert: evento },
                    { upsert: true, session }
                );
            }

            await procesados.insertOne({
                consumidor: "proyector",
                eventId: evento.eventId,
                procesadoEn: new Date(),
            }, { session });
        });
    } finally {
        await session.endSession();
    }
}

async function iniciar() {
    await mongo.connect();
    console.log("Proyector conectado a MongoDB");

    const db = mongo.db("rutasur");

    await db.collection("notificaciones").createIndex(
        { eventId: 1 },
        { unique: true }
    );

    await db.collection("alertas").createIndex(
        { shipmentId: 1, regla: 1 },
        { unique: true }
    );

    await consumer.connect();
    await producer.connect();

    for (const topic of TOPICS) {
        await consumer.subscribe({
            topic,
            fromBeginning: true,
        });
    }

    console.log("Proyector conectado a Kafka");
    console.log("Escuchando los cuatro topics...");

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({ topic, partition, message }) => {
            console.log(
                `Topic=${topic} Particion=${partition} Offset=${message.offset}`
            );

            let evento;
            let errores = [];

            try {
                evento = JSON.parse(message.value.toString());

                if (topic === "envios.estados") {
                    const resultado = validarEvento(evento);

                    if (!resultado.valido) {
                        errores = resultado.errores;
                    }
                } else if (
                    !evento ||
                    typeof evento !== "object" ||
                    Array.isArray(evento) ||
                    typeof evento.eventId !== "string" ||
                    !evento.eventId ||
                    typeof evento.shipmentId !== "string" ||
                    !evento.shipmentId ||
                    typeof evento.tipo !== "string" ||
                    !evento.tipo ||
                    typeof evento.ocurridoEn !== "string" ||
                    Number.isNaN(Date.parse(evento.ocurridoEn)) ||
                    !Number.isInteger(evento.secuencia) ||
                    typeof evento.version !== "number"
                ) {
                    errores = ["Estructura del evento inválida"];
                }
            } catch (error) {
                errores = [error.message];
            }

            if (errores.length > 0) {
                await enviarDLQ(topic, partition, message, errores);
                console.log("Evento inválido enviado a DLQ");
            } else {
                console.log(`Procesando eventId=${evento.eventId}`);
                await procesarEvento(db, topic, evento);
            }

            await consumer.commitOffsets([{
                topic,
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
            }]);
        },
    });
}

iniciar().catch((error) => {
    console.error("Error en Proyector:", error);
    process.exitCode = 1;
});

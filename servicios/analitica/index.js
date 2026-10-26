
const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const kafka = new Kafka({
    clientId: "rutasur-analitica",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "analitica",
});

const mongo = new MongoClient(
    process.env.MONGO_URL ||
    "mongodb://localhost:27017/?directConnection=true"
);

async function iniciar() {
    await mongo.connect();

    const db = mongo.db("rutasur");
    const metricas = db.collection("metricas_diarias");
    const estados = db.collection("analitica_estados");
    const procesados = db.collection("eventos_procesados");

    await estados.createIndex(
        { shipmentId: 1 },
        { unique: true }
    );

    await consumer.connect();

    await consumer.subscribe({
        topics: ["envios.estados", "envios.promesas"],
        fromBeginning: true,
    });

    console.log("Servicio Analítica iniciado");

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({ topic, partition, message }) => {
            const evento = JSON.parse(message.value.toString());
            const session = mongo.startSession();

            try {
                await session.withTransaction(async () => {
                    const existe = await procesados.findOne(
                        {
                            consumidor: "analitica",
                            eventId: evento.eventId,
                        },
                        { session }
                    );

                    if (existe) return;

                    const filtroEnvio = {
                        shipmentId: evento.shipmentId,
                    };

                    const actual = await estados.findOne(
                        filtroEnvio,
                        { session }
                    );

                    if (topic === "envios.promesas") {
                        await estados.updateOne(
                            filtroEnvio,
                            {
                                $set: {
                                    venceEn: evento.venceEn,
                                    hubId: evento.hubId,
                                },
                            },
                            { upsert: true, session }
                        );
                    } else if (
                        !actual ||
                        evento.secuencia > (actual.ultimaSecuencia ?? 0)
                    ) {
                        const fecha = evento.ocurridoEn.slice(0, 10);

                        const franja =
                            evento.franja ||
                            (new Date(evento.ocurridoEn).getUTCHours() < 12
                                ? "MANANA"
                                : "TARDE");

                        const filtroMetrica = {
                            fecha,
                            zonaId: evento.zonaId,
                            hubId: evento.hubId,
                            franja,
                        };

                        const incremento = {};

                        if (evento.tipo === "EN_REPARTO") {
                            incremento.salidasReparto = 1;

                            if (!actual?.primerIntentoRegistrado) {
                                incremento.primerosIntentos = 1;
                            }
                        }

                        if (evento.tipo === "ENTREGADO") {
                            incremento.entregados = 1;

                            if (!actual?.tuvoFallo) {
                                incremento.entregadosPrimerIntento = 1;
                            }

                            if (actual?.venceEn) {
                                const cumplio =
                                    new Date(evento.ocurridoEn) <=
                                    new Date(actual.venceEn);

                                incremento[
                                    cumplio ? "cumplidosSLA" : "incumplidosSLA"
                                ] = 1;
                            }
                        }

                        if (evento.tipo === "ENTREGA_FALLIDA") {
                            incremento.fallos = 1;

                            if (evento.motivo) {
                                incremento[`motivos.${evento.motivo}`] = 1;
                            }
                        }

                        if (Object.keys(incremento).length > 0) {
                            await metricas.updateOne(
                                filtroMetrica,
                                { $inc: incremento },
                                { upsert: true, session }
                            );
                        }

                        const cambios = {
                            ultimaSecuencia: evento.secuencia,
                            hubId: evento.hubId,
                            zonaId: evento.zonaId,
                        };

                        if (evento.tipo === "ENTREGA_FALLIDA") {
                            cambios.tuvoFallo = true;
                        }

                        if (evento.tipo === "EN_REPARTO") {
                            cambios.primerIntentoRegistrado = true;
                        }

                        await estados.updateOne(
                            filtroEnvio,
                            { $set: cambios },
                            { upsert: true, session }
                        );
                    }

                    await procesados.insertOne(
                        {
                            consumidor: "analitica",
                            eventId: evento.eventId,
                            procesadoEn: new Date(),
                        },
                        { session }
                    );
                });

                await consumer.commitOffsets([
                    {
                        topic,
                        partition,
                        offset: (BigInt(message.offset) + 1n).toString(),
                    },
                ]);

                console.log(
                    `Procesado eventId=${evento.eventId} ` +
                    `particion=${partition} offset=${message.offset}`
                );
            } finally {
                await session.endSession();
            }
        },
    });
}

iniciar().catch((error) => {
    console.error("Error en Analítica:", error);
    process.exitCode = 1;
});


const { Kafka } = require("kafkajs");
const { MongoClient } = require("mongodb");

const kafka = new Kafka({
    clientId: "rutasur-monitor-sla",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const consumer = kafka.consumer({
    groupId: "monitor-sla",
});

const producer = kafka.producer();

const mongo = new MongoClient(
    process.env.MONGO_URL ||
    "mongodb://localhost:27017/?directConnection=true"
);

const FACTOR_ACELERACION = 120;
const HORA = 60 * 60 * 1000;

let inicioReal = Date.now();
let inicioSimulado = Date.now();

function ahoraSimulado() {
    return new Date(
        inicioSimulado +
        (Date.now() - inicioReal) * FACTOR_ACELERACION
    );
}

function crearAlerta(envio, regla, prioridad, estado) {
    return {
        eventId: `sla:${envio.shipmentId}:${regla}:${estado}`,
        shipmentId: envio.shipmentId,
        tipo: "ALERTA_SLA",
        ocurridoEn: ahoraSimulado().toISOString(),
        secuencia: envio.ultimaSecuencia ?? 0,
        hubId: envio.hubId,
        zonaId: envio.zonaId,
        repartidorId: null,
        version: 1,
        regla,
        prioridad,
        estado,
    };
}

async function iniciar() {
    await mongo.connect();

    const db = mongo.db("rutasur");
    const estados = db.collection("monitor_sla_estados");
    const alertasEmitidas = db.collection("monitor_sla_alertas");

    await estados.createIndex(
        { shipmentId: 1 },
        { unique: true }
    );

    await alertasEmitidas.createIndex(
        { shipmentId: 1, regla: 1 },
        { unique: true }
    );

    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({
        topics: ["envios.estados", "envios.promesas"],
        fromBeginning: true,
    });

    console.log("Monitor SLA iniciado");

    async function publicarAlerta(envio, regla, prioridad) {
        const existente = await alertasEmitidas.findOne({
            shipmentId: envio.shipmentId,
            regla,
        });

        if (existente) return;

        const alerta = crearAlerta(
            envio,
            regla,
            prioridad,
            "ABIERTA"
        );

        await producer.send({
            topic: "alertas.sla",
            messages: [{
                key: envio.shipmentId,
                value: JSON.stringify(alerta),
            }],
        });

        await alertasEmitidas.updateOne(
            { shipmentId: envio.shipmentId, regla },
            {
                $setOnInsert: {
                    shipmentId: envio.shipmentId,
                    regla,
                    prioridad,
                    estado: "ABIERTA",
                },
            },
            { upsert: true }
        );

        console.log(
            `Alerta ${prioridad}: ${regla} - ${envio.shipmentId}`
        );
    }

    async function evaluar(envio) {
        if (!envio?.venceEn || envio.estado === "ENTREGADO") {
            return;
        }

        const ahora = ahoraSimulado();
        const vencimiento = new Date(envio.venceEn);
        const horasRestantes =
            (vencimiento - ahora) / HORA;

        if (
            horasRestantes < 10 &&
            ![
                "INGRESADO_HUB",
                "CLASIFICADO",
                "EN_REPARTO",
                "ENTREGADO",
                "ENTREGA_FALLIDA",
            ].includes(envio.estado)
        ) {
            await publicarAlerta(
                envio,
                "SIN_INGRESO",
                "MEDIA"
            );
        }

        if (
            horasRestantes < 4 &&
            !["EN_REPARTO", "ENTREGADO"].includes(envio.estado)
        ) {
            await publicarAlerta(
                envio,
                "SIN_SALIDA",
                "ALTA"
            );
        }

        if (
            envio.estado === "ENTREGA_FALLIDA" &&
            ahora.toISOString().slice(0, 10) ===
            vencimiento.toISOString().slice(0, 10)
        ) {
            await publicarAlerta(
                envio,
                "FALLO_DIA_LIMITE",
                "ALTA"
            );
        }

        if (horasRestantes < 0) {
            await publicarAlerta(
                envio,
                "VENCIDO",
                "CRITICA"
            );
        }
    }

    async function cerrarAlertas(envio) {
        const abiertas = await alertasEmitidas.find({
            shipmentId: envio.shipmentId,
            estado: "ABIERTA",
        }).toArray();

        for (const alerta of abiertas) {
            const cierre = crearAlerta(
                envio,
                alerta.regla,
                alerta.prioridad,
                "CERRADA"
            );

            await producer.send({
                topic: "alertas.sla",
                messages: [{
                    key: envio.shipmentId,
                    value: JSON.stringify(cierre),
                }],
            });

            await alertasEmitidas.updateOne(
                { _id: alerta._id },
                { $set: { estado: "CERRADA" } }
            );
        }
    }

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({ topic, partition, message }) => {
            const evento = JSON.parse(message.value.toString());

            console.log(
                `eventId=${evento.eventId} particion=${partition} offset=${message.offset}`
            );

            const actual = await estados.findOne({
                shipmentId: evento.shipmentId,
            });

            if (topic === "envios.promesas") {
                await estados.updateOne(
                    { shipmentId: evento.shipmentId },
                    {
                        $set: {
                            shipmentId: evento.shipmentId,
                            venceEn: evento.venceEn,
                            hubId: evento.hubId,
                            zonaId: evento.zonaId,
                        },
                    },
                    { upsert: true }
                );
            } else if (
                !actual ||
                evento.secuencia > (actual.ultimaSecuencia ?? 0)
            ) {
                await estados.updateOne(
                    { shipmentId: evento.shipmentId },
                    {
                        $set: {
                            shipmentId: evento.shipmentId,
                            estado: evento.tipo,
                            ultimaSecuencia: evento.secuencia,
                            hubId: evento.hubId,
                            zonaId: evento.zonaId,
                        },
                    },
                    { upsert: true }
                );
            }

            const actualizado = await estados.findOne({
                shipmentId: evento.shipmentId,
            });

            if (actualizado.estado === "ENTREGADO") {
                await cerrarAlertas(actualizado);
            } else {
                await evaluar(actualizado);
            }

            await consumer.commitOffsets([{
                topic,
                partition,
                offset: (BigInt(message.offset) + 1n).toString(),
            }]);
        },
    });

    setInterval(async () => {
        try {
            const abiertos = await estados.find({
                estado: { $ne: "ENTREGADO" },
                venceEn: { $exists: true },
            }).toArray();

            for (const envio of abiertos) {
                await evaluar(envio);
            }
        } catch (error) {
            console.error("Error evaluando SLA:", error);
        }
    }, 60000);
}

iniciar().catch(console.error);

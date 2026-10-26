const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const kafka = new Kafka({
    clientId: 'rutasur-reprocesamiento',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'reprocesamiento-historico' });
const mongo = new MongoClient(process.env.MONGO_URL || 'mongodb://localhost:27017/?replicaSet=rs0&directConnection=true');

function fechaValida(valor) {
    if (!valor) return null;
    const fecha = new Date(valor);
    return Number.isFinite(fecha.getTime()) ? fecha : null;
}

async function iniciar() {
    await mongo.connect();
    const db = mongo.db('rutasur');
    const estados = db.collection('reprocesamiento_estados');
    const tiempos = db.collection('tiempos_hub');
    await estados.createIndex({ shipmentId: 1 }, { unique: true });
    await tiempos.createIndex({ hubId: 1 }, { unique: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });
    console.log('Reprocesamiento histórico iniciado');

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            let evento;
let errorValidacion = null;

try {
    evento = JSON.parse(message.value?.toString() || '');
} catch (error) {
    errorValidacion = error.message;
}

if (
    !errorValidacion &&
    (
        !evento ||
        typeof evento.shipmentId !== 'string' ||
        !evento.shipmentId ||
        typeof evento.tipo !== 'string' ||
        !evento.tipo ||
        !fechaValida(evento.ocurridoEn)
    )
) {
    errorValidacion = 'Evento incompleto';
}

if (errorValidacion) {
    console.log(
        `Evento inválido ignorado en ${topic}/${partition}/${message.offset}: ${errorValidacion}`
    );

    await consumer.commitOffsets([{
        topic,
        partition,
        offset: (BigInt(message.offset) + 1n).toString(),
    }]);

    return;
}

            if (evento.tipo === 'INGRESADO_HUB' || evento.tipo === 'EN_REPARTO') {
                const sesion = mongo.startSession();
                try {
                    await sesion.withTransaction(async () => {
                        const filtro = { shipmentId: evento.shipmentId };
                        const actual = await estados.findOne(filtro, { session: sesion });
                        const campo = evento.tipo === 'INGRESADO_HUB' ? 'ingresoHub' : 'salidaReparto';
                        const cambios = {};
                        if (!actual?.[campo]) cambios[campo] = evento.ocurridoEn;
                        if (!actual?.hubId && evento.hubId) cambios.hubId = evento.hubId;

                        if (Object.keys(cambios).length) {
                            await estados.updateOne(filtro, {
                                $setOnInsert: { shipmentId: evento.shipmentId },
                                $set: cambios,
                            }, { upsert: true, session: sesion });
                        }

                        const nuevo = { ...actual, ...cambios };
                        const ingreso = fechaValida(nuevo.ingresoHub);
                        const salida = fechaValida(nuevo.salidaReparto);
                        const hubId = nuevo.hubId || evento.hubId;
                        if (!nuevo.procesado && ingreso && salida && hubId) {
                            const minutos = (salida - ingreso) / 60000;
                            if (minutos >= 0) {
                                await tiempos.updateOne({ hubId }, {
                                    $inc: { totalMinutos: minutos, cantidadEnvios: 1 },
                                }, { upsert: true, session: sesion });
                                await estados.updateOne(filtro, {
                                    $set: { procesado: true },
                                }, { session: sesion });
                            }
                        }
                    });
                } finally {
                    await sesion.endSession();
                }
            }

            await consumer.commitOffsets([{
                topic, partition, offset: (BigInt(message.offset) + 1n).toString(),
            }]);
        },
    });
}

iniciar().catch(error => {
    console.error('Error en reprocesamiento:', error);
    process.exitCode = 1;
});

const crypto = require("node:crypto");
const { Kafka } = require("kafkajs");
const { ESTADOS } = require("./src/estados");
const { ejecutarEscenarioAusente } = require("./src/escenarioAusente");
const { ejecutarEscenarioTercerFallo } = require("./src/escenarioTercerFallo");
const { ejecutarEscenarioDemorado } = require("./src/escenarioDemorado");
const { ejecutarEscenarioRafaga } = require("./src/escenarioRafaga");
const { iniciarGeneracionContinua } = require("./src/generacionContinua");

const kafka = new Kafka({ clientId: "rutasur-simulador", brokers: [process.env.KAFKA_BROKER || "localhost:9092"] });
const producer = kafka.producer();

async function ejecutarEscenarioFeliz() {
    const shipmentId = `ENV-${crypto.randomUUID()}`;
    const estados = [ESTADOS.CREADO, ESTADOS.INGRESADO_HUB, ESTADOS.CLASIFICADO, ESTADOS.EN_REPARTO, ESTADOS.ENTREGADO];
    for (let i = 0; i < estados.length; i++) {
        const evento = {
            eventId: crypto.randomUUID(), shipmentId, tipo: estados[i], ocurridoEn: new Date().toISOString(),
            secuencia: i + 1, hubId: "HUB-01", zonaId: "ZONA-01", repartidorId: i >= 2 ? "REP-01" : null, version: 1,
        };
        await producer.send({ topic: "envios.estados", messages: [{ key: shipmentId, value: JSON.stringify(evento) }] });
        console.log(`Evento publicado: ${evento.tipo} - Secuencia ${evento.secuencia}`);
    }
    console.log("Escenario de envío feliz completado");
}

async function iniciarSimulador() {
    let conectado = false;
    try {
        await producer.connect();
        conectado = true;
        console.log("Simulador conectado a Kafka");
        const escenario = process.argv[2] || "feliz";
        if (escenario === "feliz") await ejecutarEscenarioFeliz();
        else if (escenario === "ausente") await ejecutarEscenarioAusente(producer);
        else if (escenario === "tercer-fallo") await ejecutarEscenarioTercerFallo(producer);
        else if (escenario === "demorado") await ejecutarEscenarioDemorado(producer);
        else if (escenario === "rafaga") await ejecutarEscenarioRafaga(producer);
        else if (escenario === "continuo") {
            const detener = await iniciarGeneracionContinua(producer);
            console.log("Presioná Ctrl+C para detener el simulador.");
            await new Promise((resolve, reject) => {
                let deteniendo = false;
                const terminar = () => {
                    if (deteniendo) return;
                    deteniendo = true;
                    detener().then(resolve, reject);
                };
                process.once("SIGINT", terminar);
                process.once("SIGTERM", terminar);
            });
        } else throw new Error(`Escenario desconocido: ${escenario}`);
    } catch (error) {
        console.error("Error en el simulador:", error);
        process.exitCode = 1;
    } finally {
        if (conectado) await producer.disconnect();
    }
}

iniciarSimulador();

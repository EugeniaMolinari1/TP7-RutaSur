const crypto = require("node:crypto");

const { ESTADOS } = require("./estados");
const { ahoraSimulado } = require("./reloj");

const esperar = (ms) =>
    new Promise((resolve) => setTimeout(resolve, ms));

async function evolucionarEnvio(producer, datos) {
    const {
        shipmentId,
        hubId,
        zonaId,
        tendraFallo,
        motivoFallo,
        tendraDemora,
    } = datos;

    let secuencia = 1;

    async function publicarEstado(tipo) {
        secuencia++;

        const evento = {
            eventId: crypto.randomUUID(),
            shipmentId,
            tipo,
            ocurridoEn: ahoraSimulado().toISOString(),
            secuencia,
            hubId,
            zonaId,
            repartidorId:
                tipo === ESTADOS.EN_REPARTO ||
                    tipo === ESTADOS.ENTREGADO ||
                    tipo === ESTADOS.ENTREGA_FALLIDA
                    ? "REP-01"
                    : null,
            version: 1,
        };

        if (tipo === ESTADOS.ENTREGA_FALLIDA) {
            evento.motivo = motivoFallo;
            evento.intento = 1;
            evento.franja = "MANANA";
        }

        await producer.send({
            topic: "envios.estados",
            messages: [
                {
                    key: shipmentId,
                    value: JSON.stringify(evento),
                },
            ],
        });

        console.log(
            `${shipmentId} -> ${tipo} (secuencia ${secuencia})`
        );
    }

    await esperar(2000);
    await publicarEstado(ESTADOS.INGRESADO_HUB);

    if (tendraDemora) {
        await esperar(10000);
    } else {
        await esperar(2000);
    }

    await publicarEstado(ESTADOS.CLASIFICADO);

    await esperar(2000);
    await publicarEstado(ESTADOS.EN_REPARTO);

    await esperar(2000);

    if (tendraFallo) {
        await publicarEstado(ESTADOS.ENTREGA_FALLIDA);
    } else {
        await publicarEstado(ESTADOS.ENTREGADO);
    }
}

module.exports = { evolucionarEnvio };
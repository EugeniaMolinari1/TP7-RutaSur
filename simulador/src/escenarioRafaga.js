const crypto = require("node:crypto");
const { ESTADOS } = require("./estados");

async function ejecutarEscenarioRafaga(producer) {
    const cantidadEnvios = 500;
    const lote = 100;

    console.log(`Iniciando ráfaga de ${cantidadEnvios} envíos...`);

    let publicados = 0;

    for (let i = 0; i < cantidadEnvios; i += lote) {
        const mensajes = [];

        for (let j = i; j < Math.min(i + lote, cantidadEnvios); j++) {
            const shipmentId = `ENV-${crypto.randomUUID()}`;

            const evento = {
                eventId: crypto.randomUUID(),
                shipmentId,
                tipo: ESTADOS.CREADO,
                ocurridoEn: new Date().toISOString(),
                secuencia: 1,
                hubId: `HUB-0${(j % 3) + 1}`,
                zonaId: `ZONA-0${(j % 5) + 1}`,
                repartidorId: null,
                version: 1,
            };

            mensajes.push({
                key: shipmentId,
                value: JSON.stringify(evento),
            });
        }

        await producer.send({
            topic: "envios.estados",
            messages: mensajes,
        });

        publicados += mensajes.length;

        console.log(`Eventos publicados: ${publicados}/${cantidadEnvios}`);
    }

    console.log("Escenario Ráfaga completado");
}

module.exports = { ejecutarEscenarioRafaga };
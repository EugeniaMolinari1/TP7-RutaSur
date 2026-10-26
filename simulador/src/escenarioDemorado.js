const crypto = require("node:crypto");
const { ESTADOS } = require("./estados");
const {
    ahoraSimulado,
    sumarHorasSimuladas,
} = require("./reloj");

async function ejecutarEscenarioDemorado(producer) {
    const shipmentId = `ENV-${crypto.randomUUID()}`;

    const inicio = ahoraSimulado();
    const vencimiento = sumarHorasSimuladas(inicio, 3);

    const promesa = {
        eventId: crypto.randomUUID(),
        shipmentId,
        tipo: "PROMESA_ENTREGA",
        ocurridoEn: inicio.toISOString(),
        secuencia: 0,
        venceEn: vencimiento.toISOString(),
        hubId: "HUB-01",
        zonaId: "ZONA-01",
        repartidorId: null,
        version: 1,
    };

    await producer.send({
        topic: "envios.promesas",
        messages: [
            {
                key: shipmentId,
                value: JSON.stringify(promesa),
            },
        ],
    });

    console.log("Promesa de entrega publicada");

    const estados = [
        ESTADOS.CREADO,
        ESTADOS.INGRESADO_HUB,
    ];

    for (let i = 0; i < estados.length; i++) {
        const evento = {
            eventId: crypto.randomUUID(),
            shipmentId,
            tipo: estados[i],
            ocurridoEn: inicio.toISOString(),
            secuencia: i + 1,
            hubId: "HUB-01",
            zonaId: "ZONA-01",
            repartidorId: null,
            version: 1,
        };

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
            `Evento publicado: ${evento.tipo} - Secuencia ${evento.secuencia}`
        );
    }

    console.log("Escenario Demorado completado");
    console.log("El paquete permanece en el hub.");
    console.log("Vencimiento:", promesa.venceEn);
}

module.exports = { ejecutarEscenarioDemorado };
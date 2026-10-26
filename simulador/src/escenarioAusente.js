const crypto = require("node:crypto");
const { ESTADOS, MOTIVOS_FALLO } = require("./estados");

async function ejecutarEscenarioAusente(producer) {
    const shipmentId = `ENV-${crypto.randomUUID()}`;

    const estados = [
        ESTADOS.CREADO,
        ESTADOS.INGRESADO_HUB,
        ESTADOS.CLASIFICADO,
        ESTADOS.EN_REPARTO,
        ESTADOS.ENTREGA_FALLIDA,
    ];

    for (let i = 0; i < estados.length; i++) {
        const evento = {
            eventId: crypto.randomUUID(),
            shipmentId,
            tipo: estados[i],
            ocurridoEn: new Date().toISOString(),
            secuencia: i + 1,
            hubId: "HUB-01",
            zonaId: "ZONA-01",
            repartidorId: i >= 2 ? "REP-01" : null,
            version: 1,
        };

        if (evento.tipo === ESTADOS.ENTREGA_FALLIDA) {
            evento.motivo = MOTIVOS_FALLO.AUSENTE;
            evento.intento = 1;
            evento.franja = "MANANA";
        }

        await producer.send({
            topic: "envios.estados",
            messages: [
                {
                    key: evento.shipmentId,
                    value: JSON.stringify(evento),
                },
            ],
        });

        console.log(
            `Evento publicado: ${evento.tipo} - Secuencia ${evento.secuencia}`
        );
    }

    console.log("Escenario Ausente completado");
}

module.exports = { ejecutarEscenarioAusente };

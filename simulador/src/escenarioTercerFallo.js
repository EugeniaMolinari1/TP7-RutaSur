const crypto = require("node:crypto");
const { ESTADOS, MOTIVOS_FALLO } = require("./estados");

async function ejecutarEscenarioTercerFallo(producer) {
    const shipmentId = `ENV-${crypto.randomUUID()}`;
    let secuencia = 1;

    async function publicarEvento(tipo, intento = null) {
        const evento = {
            eventId: crypto.randomUUID(),
            shipmentId,
            tipo,
            ocurridoEn: new Date().toISOString(),
            secuencia: secuencia++,
            hubId: "HUB-01",
            zonaId: "ZONA-01",
            repartidorId: "REP-01",
            version: 1,
        };

        if (tipo === ESTADOS.ENTREGA_FALLIDA) {
            evento.motivo = MOTIVOS_FALLO.AUSENTE;
            evento.intento = intento;
            evento.franja = intento % 2 === 1 ? "MANANA" : "TARDE";
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
            `Evento publicado: ${tipo} - Secuencia ${evento.secuencia}` +
            (intento ? ` - Intento ${intento}` : "")
        );
    }

    await publicarEvento(ESTADOS.CREADO);
    await publicarEvento(ESTADOS.INGRESADO_HUB);
    await publicarEvento(ESTADOS.CLASIFICADO);

    for (let intento = 1; intento <= 3; intento++) {
        await publicarEvento(ESTADOS.EN_REPARTO);
        await publicarEvento(ESTADOS.ENTREGA_FALLIDA, intento);
    }

    console.log("Escenario Tercer Fallo completado");
    console.log("El servicio de reprogramación deberá marcar la devolución.");
}

module.exports = { ejecutarEscenarioTercerFallo };
const Ajv = require("ajv");

const ajv = new Ajv();

const estados = [
    "CREADO",
    "INGRESADO_HUB",
    "CLASIFICADO",
    "EN_REPARTO",
    "ENTREGADO",
    "ENTREGA_FALLIDA",
];

const motivos = [
    "AUSENTE",
    "DIRECCION_INCORRECTA",
    "RECHAZADO",
    "ZONA_INACCESIBLE",
];

const esquema = {
    type: "object",
    required: [
        "eventId",
        "shipmentId",
        "tipo",
        "ocurridoEn",
        "secuencia",
        "hubId",
        "zonaId",
        "version",
    ],
    properties: {
        eventId: { type: "string", minLength: 1 },
        shipmentId: { type: "string", minLength: 1 },
        tipo: { type: "string", enum: estados },
        ocurridoEn: { type: "string", minLength: 1 },
        secuencia: { type: "integer", minimum: 1 },
        hubId: { type: "string", minLength: 1 },
        zonaId: { type: "string", minLength: 1 },
        repartidorId: { type: ["string", "null"] },
        version: { type: "integer", minimum: 1 },
        motivo: { type: "string", enum: motivos },
        intento: { type: "integer", minimum: 1 },
        franja: { type: "string" },
    },
};

const validarEsquema = ajv.compile(esquema);

function validarEvento(evento) {
    if (!validarEsquema(evento)) {
        return {
            valido: false,
            errores: validarEsquema.errors,
        };
    }

    if (Number.isNaN(Date.parse(evento.ocurridoEn))) {
        return {
            valido: false,
            errores: ["Fecha ocurridoEn inválida"],
        };
    }

    if (
        evento.tipo === "ENTREGA_FALLIDA" &&
        (!evento.motivo || !evento.intento)
    ) {
        return {
            valido: false,
            errores: ["Entrega fallida sin motivo o intento"],
        };
    }

    return { valido: true, errores: [] };
}

module.exports = { validarEvento };
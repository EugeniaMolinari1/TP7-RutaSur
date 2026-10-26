const { CONFIG } = require("./config");

const FACTOR_ACELERACION = CONFIG.factorAceleracion;

const inicioReal = Date.now();
const inicioSimulado = Date.now();

function ahoraSimulado() {
    const tiempoTranscurrido = Date.now() - inicioReal;

    return new Date(
        inicioSimulado + tiempoTranscurrido * FACTOR_ACELERACION
    );
}

function sumarHorasSimuladas(fecha, horas) {
    return new Date(
        fecha.getTime() + horas * 60 * 60 * 1000
    );
}

module.exports = {
    FACTOR_ACELERACION,
    ahoraSimulado,
    sumarHorasSimuladas,
};
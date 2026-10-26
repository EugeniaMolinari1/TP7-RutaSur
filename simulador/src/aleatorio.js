const { CONFIG } = require("./config");

function crearGenerador(semilla = CONFIG.semillaAleatoria) {
    let estado = semilla >>> 0;

    function siguiente() {
        estado = (1664525 * estado + 1013904223) >>> 0;

        return estado / 4294967296;
    }

    function ocurre(probabilidad) {
        return siguiente() < probabilidad;
    }

    function elegirMotivo() {
        const numero = siguiente();
        let acumulado = 0;

        for (const [motivo, probabilidad] of Object.entries(
            CONFIG.distribucionMotivos
        )) {
            acumulado += probabilidad;

            if (numero < acumulado) {
                return motivo;
            }
        }

        return "ZONA_INACCESIBLE";
    }

    return {
        siguiente,
        ocurre,
        elegirMotivo,
    };
}

module.exports = { crearGenerador };
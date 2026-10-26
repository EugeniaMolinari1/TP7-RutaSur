const CONFIG = {
    enviosPorMinuto: 20,

    factorAceleracion: 120,

    tasaFalloPorZona: {
        "ZONA-01": 0.05,
        "ZONA-02": 0.10,
        "ZONA-03": 0.15,
        "ZONA-04": 0.20,
        "ZONA-05": 0.30,
    },

    distribucionMotivos: {
        AUSENTE: 0.60,
        DIRECCION_INCORRECTA: 0.20,
        RECHAZADO: 0.15,
        ZONA_INACCESIBLE: 0.05,
    },

    probabilidadDemoraHub: 0.10,

    probabilidadDuplicado: 0.02,

    probabilidadFueraOrden: 0.02,

    probabilidadMalformado: 0.01,

    semillaAleatoria: 12345,
};

module.exports = { CONFIG };
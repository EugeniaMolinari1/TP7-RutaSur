const crypto = require("node:crypto");

const { CONFIG } = require("./config");
const { crearGenerador } = require("./aleatorio");
const { ahoraSimulado } = require("./reloj");
const { ESTADOS } = require("./estados");
const { evolucionarEnvio } = require("./evolucionEnvio");

async function iniciarGeneracionContinua(producer) {
    const generador = crearGenerador();

    const zonas = Object.keys(CONFIG.tasaFalloPorZona);
    const intervalo = (60 * 1000) / CONFIG.enviosPorMinuto;

    let cantidadGenerada = 0;

    console.log("Generación continua iniciada");
    console.log(`Envíos por minuto: ${CONFIG.enviosPorMinuto}`);
    console.log(`Intervalo: ${intervalo} ms`);

    async function generarEnvio() {
        const shipmentId = `ENV-${crypto.randomUUID()}`;

        const indiceZona = Math.floor(
            generador.siguiente() * zonas.length
        );

        const zonaId = zonas[indiceZona];

        const hubId = `HUB-0${Math.floor(generador.siguiente() * 3) + 1
            }`;

        const probabilidadFallo =
            CONFIG.tasaFalloPorZona[zonaId];

        const tendraFallo = generador.ocurre(probabilidadFallo);

        const motivoFallo = tendraFallo
            ? generador.elegirMotivo()
            : null;

        const tendraDemora = generador.ocurre(
            CONFIG.probabilidadDemoraHub
        );

        const evento = {
            eventId: crypto.randomUUID(),
            shipmentId,
            tipo: ESTADOS.CREADO,
            ocurridoEn: ahoraSimulado().toISOString(),
            secuencia: 1,
            hubId,
            zonaId,
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

        cantidadGenerada++;

        console.log(
            `[${cantidadGenerada}] ${shipmentId} | ${zonaId} | ` +
            `Fallo previsto: ${tendraFallo ? motivoFallo : "NO"} | ` +
            `Demora prevista: ${tendraDemora ? "SI" : "NO"}`
        );
        const evolucion = evolucionarEnvio(producer, {
            shipmentId,
            hubId,
            zonaId,
            tendraFallo,
            motivoFallo,
            tendraDemora,
        });

        evolucionesPendientes.add(evolucion);

        evolucion
            .catch((error) => {
                console.error(
                    `Error evolucionando envío ${shipmentId}:`,
                    error
                );
            })
            .finally(() => {
                evolucionesPendientes.delete(evolucion);
            });
    }

    let generando = false;
    const evolucionesPendientes = new Set();

    const temporizador = setInterval(async () => {
        if (generando) return;

        generando = true;

        try {
            await generarEnvio();
        } catch (error) {
            console.error("Error generando envío:", error);
        } finally {
            generando = false;
        }
    }, intervalo);

    return async function detenerGeneracion() {
        clearInterval(temporizador);

        console.log("Esperando que terminen los envíos pendientes...");

        while (generando) {
            await new Promise((resolve) => setTimeout(resolve, 100));
        }

        await Promise.allSettled([...evolucionesPendientes]);

        console.log("Todos los envíos pendientes finalizaron.");
    };
}

module.exports = { iniciarGeneracionContinua };
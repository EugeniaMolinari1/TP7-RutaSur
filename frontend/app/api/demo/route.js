import { spawn } from "node:child_process";
import path from "node:path";

const escenariosPermitidos = new Set(["feliz", "ausente", "tercer-fallo", "demorado", "rafaga"]);
const estado = globalThis.__rutasurDemo ??= { continuo: null, deteniendo: false };

function carpetaSimulador() {
    return process.env.KAFKA_BROKER ? "/simulador" : path.resolve(process.cwd(), "..", "simulador");
}

function iniciarProceso(escenario) {
    return spawn(process.execPath, ["index.js", escenario], {
        cwd: carpetaSimulador(),
        env: process.env,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
    });
}

export async function GET() {
    return Response.json({ continuoActivo: Boolean(estado.continuo), deteniendo: estado.deteniendo });
}

export async function POST(request) {
    try {
        const { escenario, accion } = await request.json();

        if (accion === "detener") {
            if (!estado.continuo) return Response.json({ mensaje: "La generación continua ya está detenida", continuoActivo: false });
            if (estado.deteniendo) return Response.json({ mensaje: "La generación continua se está deteniendo", continuoActivo: true });
            estado.deteniendo = true;
            const proceso = estado.continuo;
            // En Windows SIGINT no es fiable para procesos secundarios de Node.
            // El simulador también atiende SIGTERM para finalizar correctamente.
            proceso.kill("SIGTERM");
            return Response.json({ mensaje: "Se solicitó detener la generación continua", continuoActivo: true });
        }

        if (escenario === "continuo") {
            if (estado.continuo) return Response.json({ mensaje: "La generación continua ya está activa", continuoActivo: true });
            const proceso = iniciarProceso("continuo");
            estado.continuo = proceso;
            estado.deteniendo = false;
            proceso.stdout.on("data", (dato) => process.stdout.write(dato));
            proceso.stderr.on("data", (dato) => process.stderr.write(dato));
            proceso.once("error", (error) => {
                console.error("No se pudo iniciar el simulador:", error);
                if (estado.continuo === proceso) estado.continuo = null;
                estado.deteniendo = false;
            });
            proceso.once("close", (codigo) => {
                console.log(`Simulador continuo finalizado (código ${codigo})`);
                if (estado.continuo === proceso) estado.continuo = null;
                estado.deteniendo = false;
            });
            return Response.json({ mensaje: "Generación continua iniciada", continuoActivo: true });
        }

        if (!escenariosPermitidos.has(escenario)) {
            return Response.json({ error: "Escenario no permitido" }, { status: 400 });
        }

        const resultado = await new Promise((resolve, reject) => {
            const proceso = iniciarProceso(escenario);
            let salida = "";
            let errores = "";
            proceso.stdout.on("data", (dato) => { salida += dato.toString(); });
            proceso.stderr.on("data", (dato) => { errores += dato.toString(); });
            proceso.once("error", reject);
            proceso.once("close", (codigo) => {
                if (codigo === 0) resolve(salida);
                else reject(new Error(errores || salida || `El simulador terminó con código ${codigo}`));
            });
        });
        return Response.json({ mensaje: `Escenario "${escenario}" ejecutado correctamente`, salida: resultado });
    } catch (error) {
        console.error("Error en demo:", error);
        return Response.json({ error: error.message || "Error al ejecutar el simulador" }, { status: 500 });
    }
}

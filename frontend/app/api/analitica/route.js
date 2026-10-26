
import { getDatabase } from "../../../lib/mongodb";
import { connection } from "next/server";

export async function GET(request) {
    await connection();
    try {
        const db = await getDatabase();
        const { searchParams } = new URL(request.url);

        const zonaId = searchParams.get("zonaId");
        const hubId = searchParams.get("hubId");
        const fecha = searchParams.get("fecha");

        const filtro = {};

        if (zonaId) filtro.zonaId = zonaId;
        if (hubId) filtro.hubId = hubId;
        if (fecha) filtro.fecha = fecha;

        const coleccion = db.collection("metricas_diarias");

        const [metricas, zonas, hubs, fechas, tiemposHub] = await Promise.all([
            coleccion.find(filtro, { projection: { _id: 0 } }).toArray(),
            coleccion.distinct("zonaId"),
            coleccion.distinct("hubId"),
            coleccion.distinct("fecha"),
            db.collection("tiempos_hub")
                .find(hubId ? { hubId } : {}, { projection: { _id: 0 } })
                .toArray(),
        ]);
        const totales = {
            primerosIntentos: 0,
            entregadosPrimerIntento: 0,
            cumplidosSLA: 0,
            incumplidosSLA: 0,
            motivos: {},
        };

        for (const metrica of metricas) {
            totales.primerosIntentos += metrica.primerosIntentos || 0;
            totales.entregadosPrimerIntento +=
                metrica.entregadosPrimerIntento || 0;
            totales.cumplidosSLA += metrica.cumplidosSLA || 0;
            totales.incumplidosSLA += metrica.incumplidosSLA || 0;

            for (const [motivo, cantidad] of Object.entries(
                metrica.motivos || {}
            )) {
                totales.motivos[motivo] =
                    (totales.motivos[motivo] || 0) + cantidad;
            }
        }

        const totalSLA =
            totales.cumplidosSLA + totales.incumplidosSLA;

        const indicadores = {
            tasaPrimerIntento:
                totales.primerosIntentos > 0
                    ? (totales.entregadosPrimerIntento /
                        totales.primerosIntentos) * 100
                    : 0,

            cumplimientoSLA:
                totalSLA > 0
                    ? (totales.cumplidosSLA / totalSLA) * 100
                    : 0,

            motivos: totales.motivos,
            entregasEvaluadasSLA: totalSLA,
        };

        return Response.json({
            indicadores,
            metricas,
            tiemposHub,
            filtros: {
                zonas: zonas.filter(Boolean).sort(),
                hubs: hubs.filter(Boolean).sort(),
                fechas: fechas.filter(Boolean).sort(),
            },
        });
    } catch (error) {
        console.error("Error en Analítica:", error);

        return Response.json(
            { error: "No se pudieron consultar las métricas" },
            { status: 500 }
        );
    }
}

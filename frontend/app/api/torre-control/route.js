
import { getDatabase } from "../../../lib/mongodb";



export async function GET() {
    try {
        const db = await getDatabase();

        const [contadores, alertas, envios] = await Promise.all([
            db.collection("envios").aggregate([
                {
                    $group: {
                        _id: "$estado",
                        cantidad: { $sum: 1 },
                    },
                },
                { $sort: { _id: 1 } },
            ]).toArray(),

            db.collection("alertas")
                .find({})
                .sort({ ocurridoEn: -1 })
                .limit(20)
                .toArray(),

            db.collection("envios")
                .find({})
                .sort({ actualizadoEn: -1 })
                .limit(10)
                .toArray(),
        ]);

        return Response.json({
            contadores,
            alertas,
            envios,
        });
    } catch (error) {
        console.error("Error en Torre de control:", error);

        return Response.json(
            { error: "No se pudieron obtener los datos" },
            { status: 500 }
        );
    }
}

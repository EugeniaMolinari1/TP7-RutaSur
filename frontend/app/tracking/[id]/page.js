
import { getDatabase } from "../../../lib/mongodb";
import { connection } from "next/server";
import ActualizacionEnVivo from "../../components/ActualizacionEnVivo";
export const instant = false;


function mostrarFecha(fecha) {
    if (!fecha) return "No disponible";

    const valor = new Date(fecha);

    return Number.isNaN(valor.getTime())
        ? "No disponible"
        : valor.toLocaleString("es-AR");
}

export default async function TrackingPublico({ params }) {
    await connection();
    const { id } = await params;
    const db = await getDatabase();

    const envio = await db.collection("envios").findOne(
        { shipmentId: id },
        {
            projection: {
                _id: 0,
                shipmentId: 1,
                estado: 1,
                promesa: 1,
            },
        }
    );

    if (!envio) {
        return (
            <main style={{ padding: 30, fontFamily: "Arial, sans-serif" }}>
                <ActualizacionEnVivo shipmentId={id} />
                <h1>RutaSur - Tracking público</h1>
                <p>Envío no encontrado. Verificá el número de seguimiento.</p>
            </main>
        );
    }

    return (
        <main
            style={{
                padding: 30,
                fontFamily: "Arial, sans-serif",
                maxWidth: 650,
                margin: "auto",
            }}
        >
            <ActualizacionEnVivo shipmentId={id} />

            <h1>RutaSur - Tracking público</h1>
            <p>Consultá el estado de tu envío.</p>

            <div
                style={{
                    border: "1px solid #ccc",
                    borderRadius: 10,
                    padding: 25,
                    marginTop: 25,
                }}
            >
                <p><strong>Número de seguimiento:</strong></p>
                <p>{envio.shipmentId}</p>

                <h2>Estado actual</h2>
                <p style={{ fontSize: 24, fontWeight: "bold" }}>
                    {envio.estado || "Sin información"}
                </p>

                <h2>Fecha estimada de entrega</h2>
                <p>{mostrarFecha(envio.promesa?.venceEn)}</p>
            </div>
        </main>
    );
}

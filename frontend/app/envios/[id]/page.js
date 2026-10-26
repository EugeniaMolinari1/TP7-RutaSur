
import Link from "next/link";
import { getDatabase } from "../../../lib/mongodb";
import ActualizacionEnVivo from "../../components/ActualizacionEnVivo";

export const instant = false;
function mostrarFecha(fecha) {
    if (!fecha) return "-";

    const valor = new Date(fecha);

    return Number.isNaN(valor.getTime())
        ? "-"
        : valor.toLocaleString("es-AR");
}

export default async function DetalleEnvio({ params }) {
    const { id } = await params;
    const db = await getDatabase();

    const [envio, alertas, notificaciones] = await Promise.all([
        db.collection("envios").findOne({ shipmentId: id }),
        db.collection("alertas").find({ shipmentId: id }).toArray(),
        db.collection("notificaciones").find({ shipmentId: id }).toArray(),
    ]);

    if (!envio) {
        return (
            <main style={{ padding: 30 }}>
                <ActualizacionEnVivo shipmentId={id} />
                <Link href="/">← Volver a Torre de control</Link>
                <h1>Envío no encontrado</h1>
                <p>No existe el envío {id}.</p>
            </main>
        );
    }

    const historial = [...(envio.historial || [])].sort(
        (a, b) => (a.secuencia || 0) - (b.secuencia || 0)
    );

    return (
        <main style={{ padding: 30, fontFamily: "Arial, sans-serif" }}>
            <ActualizacionEnVivo shipmentId={id} />

            <Link href="/">← Volver a Torre de control</Link>

            <h1>Detalle del envío</h1>

            <p><strong>ID:</strong> {envio.shipmentId}</p>
            <p><strong>Estado actual:</strong> {envio.estado}</p>
            <p>
                <strong>Última actualización:</strong>{" "}
                {mostrarFecha(envio.actualizadoEn)}
            </p>

            <h2>Línea de tiempo</h2>

            {historial.length === 0 ? (
                <p>No hay eventos registrados.</p>
            ) : (
                <ol>
                    {historial.map((evento, indice) => (
                        <li key={evento.eventId || indice}>
                            <strong>{evento.tipo}</strong>
                            <p>Fecha: {mostrarFecha(evento.ocurridoEn)}</p>
                            <p>Secuencia: {evento.secuencia ?? "-"}</p>
                        </li>
                    ))}
                </ol>
            )}

            <h2>Intentos de entrega</h2>

            {(envio.intentos || []).length === 0 ? (
                <p>No hay intentos registrados.</p>
            ) : (
                <ul>
                    {envio.intentos.map((intento, indice) => (
                        <li key={indice}>
                            Intento {intento.intento ?? indice + 1} —
                            Motivo: {intento.motivo || "-"} —
                            Fecha: {mostrarFecha(intento.ocurridoEn)}
                        </li>
                    ))}
                </ul>
            )}

            <h2>Alertas del envío</h2>

            {alertas.length === 0 ? (
                <p>No hay alertas registradas.</p>
            ) : (
                <ul>
                    {alertas.map((alerta) => (
                        <li key={String(alerta._id)}>
                            {alerta.regla || "Alerta"} —
                            {alerta.prioridad || "Sin prioridad"}
                        </li>
                    ))}
                </ul>
            )}

            <h2>Notificaciones del envío</h2>

            {notificaciones.length === 0 ? (
                <p>No hay notificaciones registradas.</p>
            ) : (
                <ul>
                    {notificaciones.map((notificacion) => (
                        <li key={String(notificacion._id)}>
                            {notificacion.tipo || "Notificación"} —
                            {mostrarFecha(
                                notificacion.ocurridoEn || notificacion.enviadoEn
                            )}
                        </li>
                    ))}
                </ul>
            )}
        </main>
    );
}

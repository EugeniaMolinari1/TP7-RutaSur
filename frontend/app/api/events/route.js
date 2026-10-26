
import { getDatabase } from "../../../lib/mongodb";

export async function GET(request) {
    const encoder = new TextEncoder();
    const db = await getDatabase();

    let changeStream;
    let intervalo;
    let cerrado = false;

    const stream = new ReadableStream({
        async start(controller) {
            function enviar(mensaje) {
                if (cerrado) return;

                try {
                    controller.enqueue(
                        encoder.encode(`data: ${JSON.stringify(mensaje)}\n\n`)
                    );
                } catch {
                    cerrar();
                }
            }

            async function cerrar() {
                if (cerrado) return;
                cerrado = true;

                clearInterval(intervalo);
                request.signal.removeEventListener("abort", cerrar);

                if (changeStream) {
                    await changeStream.close().catch(() => { });
                }

                try {
                    controller.close();
                } catch {
                    // La conexión ya puede estar cerrada.
                }
            }

            request.signal.addEventListener("abort", cerrar, { once: true });

            intervalo = setInterval(() => {
                if (!cerrado) {
                    try {
                        controller.enqueue(encoder.encode(": keepalive\n\n"));
                    } catch {
                        cerrar();
                    }
                }
            }, 15000);

            try {
                changeStream = db.watch(
                    [
                        {
                            $match: {
                                "ns.coll": {
                                    $in: ["envios", "alertas", "notificaciones"],
                                },
                            },
                        },
                    ],
                    { fullDocument: "updateLookup" }
                );

                for await (const cambio of changeStream) {
                    if (cerrado) break;

                    enviar({
                        coleccion: cambio.ns.coll,
                        operacion: cambio.operationType,
                    });
                }
            } catch (error) {
                if (!cerrado) {
                    console.error("Error SSE:", error);
                    enviar({ error: "Error al escuchar cambios" });
                    await cerrar();
                }
            }
        },

        cancel() {
            cerrado = true;
            clearInterval(intervalo);

            if (changeStream) {
                changeStream.close().catch(() => { });
            }
        },
    });

    return new Response(stream, {
        headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            Connection: "keep-alive",
            "X-Accel-Buffering": "no",
        },
    });
}

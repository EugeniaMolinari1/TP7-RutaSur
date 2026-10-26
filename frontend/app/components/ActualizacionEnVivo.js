
"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function ActualizacionEnVivo({ shipmentId }) {
    const router = useRouter();

    useEffect(() => {
        const eventos = new EventSource("/api/events");

        eventos.onmessage = (evento) => {
            try {
                const cambio = JSON.parse(evento.data);

                if (cambio.error) {
                    console.error("Error SSE:", cambio.error);
                    return;
                }

                if (
                    ["envios", "alertas", "notificaciones"].includes(
                        cambio.coleccion
                    )
                ) {
                    router.refresh();
                }
            } catch (error) {
                console.error("Error al procesar SSE:", error);
            }
        };

        return () => eventos.close();
    }, [router, shipmentId]);

    return null;
}

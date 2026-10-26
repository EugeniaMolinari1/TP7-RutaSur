"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

const escenarios = [
    { id: "feliz", nombre: "Envío feliz", descripcion: "Entrega completada correctamente." },
    { id: "ausente", nombre: "Destinatario ausente", descripcion: "Intento de entrega fallido." },
    { id: "tercer-fallo", nombre: "Tercer fallo", descripcion: "Tres intentos fallidos de entrega." },
    { id: "demorado", nombre: "Envío demorado", descripcion: "Demora que puede generar alertas SLA." },
    { id: "rafaga", nombre: "Ráfaga de eventos", descripcion: "Publicación de 500 eventos." },
    { id: "continuo", nombre: "Generación continua", descripcion: "Generación sostenida de eventos." },
];

export default function PanelDemo() {
    const [ejecutando, setEjecutando] = useState("");
    const [mensaje, setMensaje] = useState("");
    const [continuoActivo, setContinuoActivo] = useState(false);
    const [deteniendo, setDeteniendo] = useState(false);

    useEffect(() => {
        let montado = true;
        const consultar = async () => {
            try {
                const respuesta = await fetch("/api/demo", { cache: "no-store" });
                if (!respuesta.ok) return;
                const datos = await respuesta.json();
                if (montado) {
                    setContinuoActivo(datos.continuoActivo);
                    setDeteniendo(datos.deteniendo);
                }
            } catch { /* El estado se volverá a consultar. */ }
        };
        consultar();
        const intervalo = setInterval(consultar, 3000);
        return () => { montado = false; clearInterval(intervalo); };
    }, []);

    async function ejecutarEscenario(escenario) {
        setEjecutando(escenario);
        setMensaje("");
        try {
            const detener = escenario === "continuo" && continuoActivo;
            const respuesta = await fetch("/api/demo", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(detener ? { accion: "detener" } : { escenario }),
            });
            const resultado = await respuesta.json();
            if (!respuesta.ok) throw new Error(resultado.error || "No se pudo ejecutar el escenario");
            setMensaje(resultado.mensaje);
            if (escenario === "continuo") {
                setContinuoActivo(resultado.continuoActivo);
                setDeteniendo(detener);
            }
        } catch (error) {
            setMensaje(`Error: ${error.message}`);
        } finally {
            setEjecutando("");
        }
    }

    return (
        <main style={{ padding: 30, fontFamily: "Arial, sans-serif" }}>
            <Link href="/">← Volver a Torre de control</Link>
            <h1>RutaSur - Panel de demo</h1>
            <p>Escenarios disponibles para demostrar el funcionamiento del sistema.</p>
            <div style={{ display: "grid", gap: 15, maxWidth: 650 }}>
                {escenarios.map((escenario) => (
                    <div key={escenario.id} style={{ border: "1px solid #ccc", borderRadius: 8, padding: 20 }}>
                        <h2>{escenario.nombre}</h2>
                        <p>{escenario.descripcion}</p>
                        <button
                            onClick={() => ejecutarEscenario(escenario.id)}
                            disabled={Boolean(ejecutando) || (escenario.id === "continuo" && deteniendo)}
                            style={{ padding: "10px 16px", cursor: "pointer" }}
                        >
                            {ejecutando === escenario.id ? "Procesando..." :
                                escenario.id === "continuo" && continuoActivo ? "Detener generación" : "Ejecutar escenario"}
                        </button>
                    </div>
                ))}
            </div>
            {mensaje && <p role="status" style={{ marginTop: 25 }}>{mensaje}</p>}
        </main>
    );
}

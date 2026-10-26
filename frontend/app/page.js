
"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

export default function Home() {
  const [datos, setDatos] = useState(null);
  const [error, setError] = useState("");


  useEffect(() => {
    let activo = true;

    async function cargarDatos() {
      try {
        const respuesta = await fetch("/api/torre-control", {
          cache: "no-store",
        });

        if (!respuesta.ok) {
          throw new Error("No se pudieron cargar los datos");
        }

        const resultado = await respuesta.json();

        if (activo) {
          setDatos(resultado);
          setError("");
        }
      } catch (err) {
        if (activo) {
          setError(err.message);
        }
      }
    }

    cargarDatos();

    const eventos = new EventSource("/api/events");

    eventos.onmessage = (evento) => {
      try {
        const cambio = JSON.parse(evento.data);

        if (
          ["envios", "alertas", "notificaciones"].includes(
            cambio.coleccion
          )
        ) {
          cargarDatos();
        }
      } catch (error) {
        console.error("Error al procesar evento SSE:", error);
      }
    };

    return () => {
      activo = false;
      eventos.close();
    };
  }, []);


  if (error) {
    return (
      <main style={{ padding: "30px" }}>
        <h2>Error al cargar los datos</h2>
        <p>{error}</p>
      </main>
    );
  }

  if (!datos) {
    return (
      <main style={{ padding: "30px" }}>
        <p>Cargando datos...</p>
      </main>
    );
  }

  return (
    <main
      style={{
        padding: "30px",
        fontFamily: "Arial, sans-serif",
      }}
    >
      <h1>RutaSur - Torre de control</h1>
      <p>Supervisión de envíos y alertas</p>

      <h2>Contadores por estado</h2>

      <div
        style={{
          display: "flex",
          gap: "15px",
          flexWrap: "wrap",
        }}
      >
        {datos.contadores.map((item) => (
          <div
            key={item._id}
            style={{
              border: "1px solid #ccc",
              borderRadius: "8px",
              padding: "15px",
              minWidth: "150px",
            }}
          >
            <strong>{item._id || "SIN ESTADO"}</strong>
            <h2>{item.cantidad}</h2>
          </div>
        ))}
      </div>

      <h2>Alertas</h2>

      <table
        border="1"
        cellPadding="10"
        style={{ width: "100%" }}
      >
        <thead>
          <tr>
            <th>Envío</th>
            <th>Tipo</th>
            <th>Severidad</th>
          </tr>
        </thead>

        <tbody>
          {datos.alertas.length === 0 ? (
            <tr>
              <td colSpan="3">No hay alertas registradas.</td>
            </tr>
          ) : (
            datos.alertas.map((alerta) => (
              <tr key={String(alerta._id)}>
                <td>{alerta.shipmentId || "-"}</td>
                <td>{alerta.regla || "-"}</td>
                <td>{alerta.prioridad || "-"}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>

      <h2>Últimos envíos actualizados</h2>

      <table
        border="1"
        cellPadding="10"
        style={{ width: "100%" }}
      >
        <thead>
          <tr>
            <th>ID del envío</th>
            <th>Estado</th>
            <th>Última secuencia</th>
          </tr>
        </thead>

        <tbody>
          {datos.envios.length === 0 ? (
            <tr>
              <td colSpan="3">No hay envíos registrados.</td>
            </tr>
          ) : (
            datos.envios.map((envio) => (
              <tr key={String(envio._id)}>
                <td>
                  <Link
                    href={`/envios/${encodeURIComponent(envio.shipmentId)}`}
                    style={{
                      color: "#0066cc",
                      textDecoration: "underline",
                      cursor: "pointer",
                      fontWeight: "bold",
                    }}
                  >
                    {envio.shipmentId}
                  </Link>
                </td>

                <td>{envio.estado || "-"}</td>

                <td>{envio.ultimaSecuencia ?? "-"}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </main>
  );
}

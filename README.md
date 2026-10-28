
# TP7 – RutaSur

**Programación Avanzada – 2026**

## Descripción

RutaSur es una prueba de concepto de un sistema logístico orientado a eventos.

Utiliza Apache Kafka para comunicar los distintos servicios, MongoDB para almacenar la información y Next.js para visualizar el estado de los envíos, las alertas y los indicadores.

## Tecnologías utilizadas

- Node.js
- Next.js
- Apache Kafka
- MongoDB
- Docker y Docker Compose

## Arquitectura

El sistema incluye los siguientes componentes:

- **Kafka:** comunicación mediante eventos.
- **MongoDB:** persistencia de datos.
- **Proyector:** actualización del estado de los envíos.
- **Notificaciones:** procesamiento de notificaciones.
- **Reprogramación:** gestión de intentos fallidos.
- **Monitor SLA:** detección de incumplimientos.
- **Analítica:** cálculo de indicadores.
- **Reprocesamiento histórico:** cálculo de métricas a partir de eventos anteriores.
- **Frontend:** torre de control, tracking público, analítica y panel de demostración.

## Requisitos previos

- Docker Desktop instalado y en funcionamiento.
- Docker Compose disponible.
- Recursos suficientes para ejecutar Kafka, MongoDB, los microservicios y Next.js.

## Ejecución del proyecto

Desde la carpeta principal del repositorio, ejecutar:

```bash
docker compose up -d --build
```

Consultar el estado de los servicios:

```bash
docker compose ps
```

Cuando los servicios estén disponibles, abrir:

**http://localhost:3000**

## Funcionalidades del frontend

- **Torre de control:** `/`
- **Detalle de un envío:** `/envios/ID_DEL_ENVIO`
- **Tracking público:** `/tracking/ID_DEL_ENVIO`
- **Analítica:** `/analitica`
- **Panel de demostración:** `/demo`

## Escenarios de demostración

Desde el panel `/demo` se pueden ejecutar:

1. Envío feliz.
2. Destinatario ausente.
3. Tercer fallo de entrega.
4. Envío demorado.
5. Ráfaga de 500 eventos.
6. Generación continua de eventos, con opción de detenerla.

Los escenarios permiten observar el procesamiento de eventos y sus efectos en el sistema.

## Reprocesamiento histórico

El servicio de reprocesamiento histórico utiliza un perfil opcional de Docker Compose.

Para ejecutarlo:

```bash
docker compose --profile reprocesamiento up -d reprocesamiento
```

Este servicio permite calcular el indicador de tiempo medio de permanencia en hub a partir de eventos históricos de Kafka.

## Detener el sistema

```bash
docker compose down
```

Este comando detiene y elimina los contenedores del proyecto sin eliminar los volúmenes de datos.

## Pruebas

El archivo `PRUEBAS.md` contiene información sobre las pruebas realizadas durante el desarrollo.

# PRUEBAS – TP7 RutaSur

## 1. Consideraciones previas

El proyecto utiliza Docker Compose, Apache Kafka, MongoDB, Node.js y Next.js.

Las pruebas funcionales documentadas se realizaron sobre una copia del proyecto en otro equipo, debido a las limitaciones de memoria de la computadora de desarrollo.

## 2. Preparación del entorno

Desde la carpeta raíz del proyecto, iniciar Kafka y MongoDB:

```bash
docker compose up -d kafka mongodb
```

Ejecutar los servicios de inicialización:

```bash
docker compose run --rm kafka-init
docker compose run --rm mongo-init
```

Comprobar el estado de los contenedores:

```bash
docker compose ps -a
```

## 3. Pruebas de consumidores

### 3.1. Notificaciones y reprogramación

Iniciar los servicios:

```bash
docker compose up -d --build notificaciones reprogramacion
```

Consultar los registros:

```bash
docker compose logs --tail=60 notificaciones reprogramacion
```

Ejecutar los escenarios:

```bash
cd simulador
node index.js ausente
node index.js tercer-fallo
cd ..
```

Verificar el procesamiento:

```bash
docker compose logs --tail=80 notificaciones reprogramacion
```

Si es necesario liberar recursos:

```bash
docker compose stop notificaciones reprogramacion
```

### 3.2. Monitor SLA y analítica

Iniciar los servicios:

```bash
docker compose up -d --build monitor-sla analitica
```

Consultar los registros:

```bash
docker compose logs --tail=50 monitor-sla analitica
```

Ejecutar el escenario demorado:

```bash
cd simulador
node index.js demorado
cd ..
```

Verificar las alertas y el procesamiento:

```bash
docker compose logs --tail=80 monitor-sla analitica
```

Si es necesario liberar recursos:

```bash
docker compose stop monitor-sla analitica
```

## 4. Reprocesamiento histórico

Consultar los valores existentes en MongoDB:

```bash
docker compose exec mongodb mongosh rutasur --quiet --eval "db.tiempos_hub.find({},{_id:0}).toArray()"
```

Iniciar el servicio:

```bash
docker compose --profile reprocesamiento up -d --build reprocesamiento
```

Consultar sus registros:

```bash
docker compose logs --tail=50 reprocesamiento
```

Reiniciar el consumidor:

```bash
docker compose restart reprocesamiento
```

Consultar nuevamente los datos:

```bash
docker compose exec mongodb mongosh rutasur --quiet --eval "db.tiempos_hub.find({},{_id:0}).toArray()"
```

En las pruebas realizadas se verificó que el servicio genera registros de tiempos de permanencia en los hubs. 

## 5. Pruebas funcionales realizadas

### 5.1. Envío feliz

Se ejecutó el escenario de entrega exitosa.

**Resultado:** el envío alcanzó el estado `ENTREGADO` y se comprobó un historial de cinco eventos.

### 5.2. Destinatario ausente

Se simuló una entrega fallida por ausencia del destinatario.

**Resultado:** se comprobó la generación de una notificación y la reprogramación de la entrega.

### 5.3. Tercer fallo de entrega

Se ejecutó el escenario correspondiente al tercer intento fallido.

**Resultado:** se verificó el procesamiento de la devolución del envío.

### 5.4. Envío demorado

Se ejecutó el escenario de incumplimiento de tiempos de entrega.

**Resultado:** se observaron alertas SLA con severidades MEDIA, ALTA y CRÍTICA.

### 5.5. Ráfaga de 500 envíos

Se ejecutó el escenario de generación masiva.

**Resultado:** se comprobó la generación de 500 nuevos envíos.

### 5.6. Generación continua

Se ejecutó el escenario continuo.

**Resultado:** se observó el incremento de la cantidad de eventos procesados.

### 5.7. Dead Letter Queue (DLQ)

Se envió un evento inválido al sistema.

**Resultado:** el evento fue registrado en la DLQ junto con el error de validación y los datos de origen.

### 5.8. Eventos duplicados y fuera de orden

Se enviaron eventos duplicados y fuera de secuencia para un envío previamente entregado.

**Resultado:** el historial del envío permaneció sin modificaciones incorrectas.

### 5.9. Recuperación del consumidor de notificaciones

Se detuvo el servicio de notificaciones, se generó un nuevo evento y posteriormente se reinició el consumidor.

**Resultado:** se comprobó que el evento pendiente fue procesado y que se registró una notificación en MongoDB.

### 5.10. Persistencia en MongoDB

Se consultaron colecciones relacionadas con envíos, notificaciones y métricas.

**Resultado:** se verificó la existencia de registros generados por el procesamiento de eventos.

## 6. Verificación del frontend

El frontend de RutaSur incluye las siguientes pantallas:

- `/`: torre de control con estados, alertas y envíos recientes.
- `/envios/ID_DEL_ENVIO`: detalle del envío e historial de eventos.
- `/tracking/ID_DEL_ENVIO`: seguimiento público del estado del envío.
- `/analitica`: indicadores y métricas.
- `/demo`: panel de escenarios de demostración.

## 7. Verificación de Docker Compose

Durante las pruebas realizadas en el otro equipo se ejecutó:

```bash
docker compose ps -a
```

**Resultado observado:**

- Kafka: en ejecución y saludable.
- MongoDB: en ejecución y saludable.
- Frontend: en ejecución.
- Proyector: en ejecución.
- Notificaciones: en ejecución.
- Reprogramación: en ejecución.
- Monitor SLA: en ejecución.
- Analítica: en ejecución.
- Reprocesamiento: en ejecución.
- Kafka-init y mongo-init: finalizados correctamente con código 0.

Estas verificaciones corresponden al entorno utilizado para las pruebas, no a una nueva ejecución en la computadora de desarrollo.


## 8. Conclusión

Las pruebas realizadas sobre una copia del proyecto permitieron comprobar el funcionamiento de los principales componentes de RutaSur: comunicación mediante Kafka, persistencia en MongoDB, procesamiento de eventos, generación de alertas, notificaciones, reprogramaciones y reprocesamiento histórico.

Se verificó el funcionamiento de los contenedores principales y la generación de información mediante los distintos escenarios del simulador.

En el proyecto original se incorporaron las correcciones técnicas de configuración de Kafka, compilación de Next.js y formato de los scripts de inicialización, manteniendo el diseño original del frontend.


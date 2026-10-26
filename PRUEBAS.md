PRUEBAS
No ejecutar todos los servicios a la vez si la memoria del equipo está comprometida.
Preparación
Desde la raíz del proyecto:
docker compose up -d kafka mongodb
docker compose run --rm kafka-init
docker compose run --rm mongo-init
​
Consumidores
Levantar de a uno o dos, verificar logs, luego detener antes de continuar:
docker compose up -d --build notificaciones reprogramacion
docker compose logs --tail=60 notificaciones reprogramacion
cd simulador
node index.js ausente
node index.js tercer-fallo
cd ..
docker compose logs --tail=80 notificaciones reprogramacion
docker compose stop notificaciones reprogramacion
​
docker compose up -d --build monitor-sla analitica
docker compose logs --tail=50 monitor-sla analitica
cd simulador
node index.js demorado
cd ..
docker compose logs --tail=80 monitor-sla analitica
docker compose stop monitor-sla analitica
​
Reprocesamiento histórico
Antes de iniciar, obtener valores previos:
docker compose exec mongodb mongosh rutasur --quiet --eval "db.tiempos_hub.find({},{_id:0}).toArray()"
docker compose --profile reprocesamiento up -d --build reprocesamiento
docker compose logs --tail=50 reprocesamiento
​
Reiniciar el consumidor y verificar que cantidadEnvios y totalMinutos no aumentan por eventos ya contabilizados:
docker compose restart reprocesamiento
docker compose exec mongodb mongosh rutasur --quiet --eval "db.tiempos_hub.find({},{_id:0}).toArray()"
​
Pendiente de verificación
Validar las cuatro reglas SLA con fechas controladas.
Comprobar DLQ, duplicados, eventos fuera de orden y ráfaga de 500.
Confirmar corrección de cumplimiento SLA con promesas tardías.
Probar inicio/detención de escenario continuo desde UI.
Comprobar SSE, persistencia y arranque completo con un solo comando.

#!/bin/sh
set -e

echo "Verificando replica set de MongoDB..."

until mongosh --host mongodb:27017 --quiet \
  --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
  sleep 2
done

ESTADO=$(mongosh --host mongodb:27017 --quiet \
  --eval "try { rs.status().ok } catch (e) { 0 }")

if [ "$ESTADO" = "1" ]; then
  echo "Replica set ya inicializado."
else
  echo "Inicializando replica set..."

  mongosh --host mongodb:27017 --quiet --eval '
    rs.initiate({
      _id: "rs0",
      members: [
        { _id: 0, host: "mongodb:27017" }
      ]
    })
  '
fi

echo "Esperando que MongoDB tenga un nodo primario..."

until mongosh --host mongodb:27017 --quiet \
  --eval "db.hello().isWritablePrimary" 2>/dev/null | grep -q "true"; do
  sleep 2
done

echo "MongoDB listo."

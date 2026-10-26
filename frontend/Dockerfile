
FROM node:24-alpine

WORKDIR /app

COPY frontend/package*.json ./

RUN npm ci

COPY frontend/ ./

RUN npm run build

COPY simulador /simulador

RUN cd /simulador && npm ci --omit=dev

EXPOSE 3000

CMD ["npm", "run", "start"]

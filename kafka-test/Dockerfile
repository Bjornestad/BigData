FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install --only=production

COPY producer.js ./

CMD ["node", "producer.js"]

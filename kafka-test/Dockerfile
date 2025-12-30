FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

# Use npm install instead of npm ci because package-lock.json might be missing
RUN npm install --only=production

COPY producer.js ./

CMD ["node", "producer.js"]
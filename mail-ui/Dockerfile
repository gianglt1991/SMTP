FROM node:18 AS builder
WORKDIR /app
COPY package.json ./
RUN [ -f package-lock.json ] && npm ci || npm install
COPY . .
RUN npm run build

FROM node:18-alpine
RUN npm install -g serve
WORKDIR /app
COPY --from=builder /app/build /app/build
RUN adduser -D appuser
USER appuser
EXPOSE ${UI_PORT:-5050}
CMD ["serve", "-s", "build", "-l", "${UI_PORT:-5050}"]

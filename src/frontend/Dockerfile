FROM node:24-alpine AS build
WORKDIR /app
COPY src/frontend/package.json src/frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY src/frontend/ ./
RUN npm run build

FROM nginx:1.28-alpine
COPY src/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 CMD wget -q -O /dev/null http://127.0.0.1/health || exit 1

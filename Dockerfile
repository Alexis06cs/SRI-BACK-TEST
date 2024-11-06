# Usar una imagen base de Node.js
FROM node:14

# Crear un directorio de trabajo
WORKDIR /app

# Copiar el package.json y package-lock.json
COPY package*.json ./

# Instalar dependencias
RUN npm install

# Copiar el resto del código de la aplicación
COPY . .

# Establecer la variable de entorno para el puerto
ENV PORT 8080

# Exponer el puerto que usará la aplicación
EXPOSE 8080

# Comando para iniciar la aplicación
CMD ["npm", "start"]

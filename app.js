const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');  // Asegúrate de importar cors

const app = express();
const bigquery = new BigQuery();

//process.env.GOOGLE_APPLICATION_CREDENTIALS = "/app/credentials/sri-f6178b26dbbf.json";

app.use(cors({
  origin: 'http://localhost:5173',
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));


// Ruta para obtener la información de los usuarios únicos
app.get('/usuarios', async (req, res) => {
  const query = `
    SELECT 
      usuario,
      ARRAY_AGG(tipo_usuario LIMIT 1)[OFFSET(0)] AS tipo_usuario,
      ARRAY_AGG(perfil LIMIT 1)[OFFSET(0)] AS perfil,
      ARRAY_AGG(objeto_autorizacion LIMIT 1)[OFFSET(0)] AS objeto_autorizacion,
      ARRAY_AGG(ambito_autorizacion LIMIT 1)[OFFSET(0)] AS ambito_autorizacion,
      ARRAY_AGG(valor_autorizacion1 LIMIT 1)[OFFSET(0)] AS valor_autorizacion1,
      ARRAY_AGG(rol LIMIT 1)[OFFSET(0)] AS rol
    FROM \`induccioncps.sri.data_werehose\`
    GROUP BY usuario
    ORDER BY usuario ASC
  `;

  try {
    const [job] = await bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows);
  } catch (error) {
    console.error('Error ejecutando la consulta:', error.message, error);
    res.status(500).send('Error al obtener los usuarios');
  }
});

// Ruta para obtener el conteo de cada nivel de riesgo
app.get('/niveles_riesgo', async (req, res) => {
    const query = `
      SELECT 
        nivel_de_riesgo, 
        COUNT(*) AS total
      FROM \`induccioncps.sri.data_werehose\`
      GROUP BY nivel_de_riesgo
      ORDER BY nivel_de_riesgo ASC
    `;
  
    try {
      const [job] = await bigquery.createQueryJob({ query });
      const [rows] = await job.getQueryResults();
      res.status(200).json(rows);
    } catch (error) {
      console.error('Error ejecutando la consulta:', error.message, error);
      res.status(500).send('Error al obtener los niveles de riesgo');
    }
  });
  // Ruta para obtener el conteo de cada valor único en desc_proc_emp
app.get('/desc_proc_emp', async (req, res) => {
  const query = `
    SELECT 
      desc_proc_emp, 
      COUNT(*) AS total
    FROM \`induccioncps.sri.data_werehose\`
    GROUP BY desc_proc_emp
    ORDER BY total DESC
  `;

  try {
    const [job] = await bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows);
  } catch (error) {
    console.error('Error ejecutando la consulta:', error.message, error);
    res.status(500).send('Error al obtener los datos de desc_proc_emp');
  }
});

// Ruta para obtener el conteo de cada usuario
app.get('/usuarios_frecuencia', async (req, res) => {
  const query = `
    SELECT 
      usuario, 
      COUNT(*) AS total
    FROM \`induccioncps.sri.data_werehose\`
    GROUP BY usuario
    ORDER BY total DESC
  `;

  try {
    const [job] = await bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows);
  } catch (error) {
    console.error('Error ejecutando la consulta:', error.message, error);
    res.status(500).send('Error al obtener los datos de usuarios');
  }
});

// Ruta para obtener el conteo de usuarios por cada proceso empresarial, nivel de riesgo y descripción del riesgo en orden descendente de total de usuarios
app.get('/usuarios_por_proceso', async (req, res) => {
  const query = `
    SELECT 
      desc_proc_emp, 
      nivel_de_riesgo,
      descripcion_riesgo,
      COUNT(DISTINCT usuario) AS total_usuarios
    FROM \`induccioncps.sri.data_werehose\`
    GROUP BY desc_proc_emp, nivel_de_riesgo, descripcion_riesgo
    ORDER BY total_usuarios DESC
  `;

  try {
    const [job] = await bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows);
  } catch (error) {
    console.error('Error ejecutando la consulta:', error.message, error);
    res.status(500).send('Error al obtener los datos de usuarios por proceso empresarial');
  }
});


// Configura el puerto del servidor
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Servidor escuchando en http://localhost:${PORT}`);
});

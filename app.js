const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const cors = require('cors');

const app = express();

app.use(cors({
  origin: 'http://localhost:5173', // Actualiza esto si tu frontend está en otro dominio en producción
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// Ruta raíz para verificar el funcionamiento del servidor
app.get('/', (req, res) => {
  res.send('¡Servidor funcionando correctamente!');
});
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
//Ruta para obtener el conteo de usuarion con la condicion 'SOD'
app.get('/conteoSOD', async (req, res) => {
  const query = `
  SELECT COUNT(*) AS TOTAL_SOD
  FROM (
    SELECT DISTINCT usuario, riesgo, tipo_riesgo
    FROM \`induccioncps.sri.data_werehose\`
    WHERE tipo_riesgo = 'SOD'
    ) AS subquey;
  `
  try{
    const [job] = await bigquery.createQueryJob({query});
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows[0]);
    }catch(error){
      console.error('Error ejecutado consultado SOD:', error.message, error);
      res.status(500).send('Error al obtener el conteo de usuarios con riesgo SOD');
    }
  });
// Ruta para obtener el conteo de usuario con la condicion 'AC' 
app.get('/conteoAC', async (req, res) => {
  const query = `
  SELECT COUNT(*) AS TOTAL_SOD
  FROM (
    SELECT DISTINCT usuario, riesgo, tipo_riesgo
    FROM \`induccioncps.sri.data_werehose\`
    WHERE tipo_riesgo = 'AC'
    ) AS subquey;
  `
  try{
    const [job] = await bigquery.createQueryJob({query});
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows[0]);
    }catch(error){
      console.error('Error ejecutado consultado AC:', error.message, error);
      res.status(500).send('Error al obtener el conteo de usuarios con riesgo AC');
    }
  });

// Ruta para obtener todo los usuarios en total 
app.get('/totalUsers',async(req, res) => {
  const query = `
    SELECT COUNT(*) AS TOTAL_USER
    FROM(
    SELECT distinct usuario FROM \`induccioncps.sri.data_werehose\`) AS subquery;`
    try {
      const [job] = await bigquery.createQueryJob({query});
      const [rows] = await job.getQueryResults();
      res.status(200).json(rows[0]);
    } catch (error) {
      console.error('Error al ejecutar la consulta para traer los usuarios', error.message, error);
      res.status(500).send('Error al obtener los usuarios');
    }
  });

// Ruta para obtener los usuarios con riesgo
app.get('/usuarios_riesgos', async (req, res) => {
  const query =
  `SELECT COUNT(*) AS USUARIOS_RIESGOS
         FROM(
          SELECT usuario,riesgo,desc_proc_emp
          FROM \`induccioncps.sri.data_werehose\`
         ) AS subquery`
      try{
        const [job] = await bigquery.createQueryJob({query});
        const [rows] = await job.getQueryResults();
        res.status(200).json(rows[0]);
        }catch(error){
        console.error(`Error al ejecutar la consulta para traer los usuarios` , error.message, error);
        res.status(500).send (`Error al obtener los usuarios`);    
        }
});

app.get('/usuarios_licencias', async (req, res) => {
  const query = `
    WITH LicenciasOrdenadas AS (
      SELECT 
          usuario,
          Licencia,
          ROW_NUMBER() OVER(
              PARTITION BY usuario 
              ORDER BY 
                  CASE 
                      WHEN Licencia = 'GB Advanced Use' THEN 1
                      WHEN Licencia = 'GC Core Use' THEN 2
                      WHEN Licencia = 'GD Self-Service Use' THEN 3
                      ELSE 4
                  END
          ) AS rn
      FROM 
          \`induccioncps.sri.data_fues\`
    )

    SELECT 
        Licencia,
        COUNT(*) AS total_usuarios
    FROM 
        LicenciasOrdenadas
    WHERE 
        rn = 1
    GROUP BY 
        Licencia
    ORDER BY 
        total_usuarios DESC;
  `;

  try {
    const [job] = await bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows);
  } catch (error) {
    console.error('Error al ejecutar la consulta para traer los usuarios por licencia', error.message, error);
    res.status(500).send('Error al obtener el conteo de usuarios por licencia');
  }
});

app.get('/usuarios_licencia_detalle', async (req, res) => {
  const query = `
    WITH LicenciasOrdenadas AS (
      SELECT 
          usuario,
          Licencia,
          ROW_NUMBER() OVER(
              PARTITION BY usuario 
              ORDER BY 
                  CASE 
                      WHEN Licencia = 'GB Advanced Use' THEN 1
                      WHEN Licencia = 'GC Core Use' THEN 2
                      WHEN Licencia = 'GD Self-Service Use' THEN 3
                      ELSE 4
                  END
          ) AS rn
      FROM 
          \`induccioncps.sri.data_fues\`
    )

    SELECT 
        usuario,
        Licencia
    FROM 
        LicenciasOrdenadas
    WHERE 
        rn = 1
    ORDER BY 
        CASE 
            WHEN Licencia = 'GB Advanced Use' THEN 1
            WHEN Licencia = 'GC Core Use' THEN 2
            WHEN Licencia = 'GD Self-Service Use' THEN 3
            ELSE 4
        END,
        usuario
    LIMIT 5;
  `;

  try {
    const [job] = await bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();
    res.status(200).json(rows);
  } catch (error) {
    console.error('Error al ejecutar la consulta para la vista detallada de usuarios', error.message, error);
    res.status(500).send('Error al obtener la vista detallada de usuarios con la licencia más cara');
  }
});



const PORT = process.env.PORT || 5000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor escuchando en http://localhost:${PORT}`);
});
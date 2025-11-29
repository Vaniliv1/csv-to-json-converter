require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

// PostgreSQL connection pool using .env variables
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

// Test database connection
pool.connect((err, client, release) => {
  if (err) {
    console.error('Error connecting to the database:', err.stack);
  } else {
    console.log('Successfully connected to PostgreSQL database');
    release();
  }
});

// Middleware
app.use(express.json());

// Root route - API info
app.get('/', (req, res) => {
    res.json({
        message: 'CSV to JSON Converter API',
        endpoints: {
            'GET /users': 'Get all users as JSON from database',
            'POST /process-csv': 'Process CSV file and save to database',
            'GET /age-distribution': 'Get age distribution of users'
        }
    });
});

// Get all users as JSON
app.get('/users', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM users ORDER BY id');
    
    // Transform to nested JSON format matching your CSV structure
    const users = result.rows.map(user => ({
      name: {
        firstName: user.first_name,
        lastName: user.last_name
      },
      age: user.age,
      address: {
        line1: user.address_line1,
        line2: user.address_line2,
        city: user.city,
        state: user.state
      },
      gender: user.gender
    }));
    
    res.json(users);
  } catch (err) {
    console.error('Error fetching users:', err);
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

// Process CSV file and import to database
app.post('/process-csv', async (req, res) => {
  try {
    // Use CSV_FILE_PATH from .env
    const csvPath = path.resolve(process.env.CSV_FILE_PATH);
    
    console.log('Looking for CSV file at:', csvPath);
    
    // Check if file exists
    if (!fs.existsSync(csvPath)) {
      return res.status(404).json({ 
        error: 'CSV file not found',
        path: csvPath,
        message: 'Please ensure users.csv exists at the specified path in .env'
      });
    }
    
    // Read CSV file
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const lines = csvContent.split('\n').filter(line => line.trim());
    
    if (lines.length < 2) {
      return res.status(400).json({ error: 'CSV file is empty or has no data rows' });
    }
    
    // Skip header line
    const dataLines = lines.slice(1);
    
    let insertedCount = 0;
    let errors = [];
    
    for (let i = 0; i < dataLines.length; i++) {
      const line = dataLines[i];
      const values = line.split(',');
      
      if (values.length >= 8) {
        try {
          await pool.query(
            `INSERT INTO users (first_name, last_name, age, address_line1, address_line2, city, state, gender)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [
              values[0].trim(), // firstName
              values[1].trim(), // lastName
              parseInt(values[2].trim()), // age
              values[3].trim(), // address.line1
              values[4].trim(), // address.line2
              values[5].trim(), // city
              values[6].trim(), // state
              values[7].trim()  // gender
            ]
          );
          insertedCount++;
        } catch (err) {
          errors.push({ line: i + 2, error: err.message });
        }
      } else {
        errors.push({ line: i + 2, error: 'Insufficient columns' });
      }
    }
    
    res.json({ 
      message: 'CSV processing completed',
      recordsInserted: insertedCount,
      totalLines: dataLines.length,
      errors: errors.length > 0 ? errors : undefined
    });
    
  } catch (err) {
    console.error('Error processing CSV:', err);
    res.status(500).json({ error: 'Error processing CSV file', details: err.message });
  }
});

// Get age distribution
app.get('/age-distribution', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        CASE 
          WHEN age < 20 THEN '< 20'
          WHEN age BETWEEN 20 AND 40 THEN '20 to 40'
          WHEN age BETWEEN 41 AND 60 THEN '40 to 60'
          ELSE '> 60'
        END as age_group,
        COUNT(*) as count
      FROM users
      GROUP BY age_group
      ORDER BY 
        CASE 
          WHEN age < 20 THEN 1
          WHEN age BETWEEN 20 AND 40 THEN 2
          WHEN age BETWEEN 41 AND 60 THEN 3
          ELSE 4
        END
    `);
    
    res.json(result.rows);
  } catch (err) {
    console.error('Error getting age distribution:', err);
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing HTTP server');
  pool.end(() => {
    console.log('Database pool closed');
  });
});

// Start server
app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
  console.log(`Database: ${process.env.DB_NAME} on ${process.env.DB_HOST}:${process.env.DB_PORT}`);
  console.log(`CSV file path: ${process.env.CSV_FILE_PATH}`);
});
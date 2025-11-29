const { Pool } = require('pg');
require('dotenv').config();

class DBService {
    constructor() {
        this.pool = new Pool({
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            database: process.env.DB_NAME,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD
        });
    }
    
    async insertUsers(records) {
        const client = await this.pool.connect();
        
        try {
            await client.query('BEGIN');
            
            let insertedCount = 0;
            for (const record of records) {
                const { name, age, address, additionalInfo } = this.transformRecord(record);
                
                await client.query(
                    `INSERT INTO users (name, age, address, additional_info) 
                     VALUES ($1, $2, $3, $4)`,
                    [name, age, address, additionalInfo]
                );
                insertedCount++;
                
                // Progress indicator for large files
                if (insertedCount % 1000 === 0) {
                    console.log(`Inserted ${insertedCount} records...`);
                }
            }
            
            await client.query('COMMIT');
            console.log(`Successfully inserted ${records.length} records`);
        } catch (error) {
            await client.query('ROLLBACK');
            console.error('Error inserting records:', error);
            throw error;
        } finally {
            client.release();
        }
    }
    
    transformRecord(record) {
        const name = `${record.name?.firstName || ''} ${record.name?.lastName || ''}`.trim();
        const age = parseInt(record.age) || 0;
        const address = record.address ? JSON.stringify(record.address) : null;
        
        const additionalInfo = {};
        for (const key in record) {
            if (key !== 'name' && key !== 'age' && key !== 'address') {
                additionalInfo[key] = record[key];
            }
        }
        
        return {
            name,
            age,
            address,
            additionalInfo: Object.keys(additionalInfo).length > 0 ? JSON.stringify(additionalInfo) : null
        };
    }
    
    async getAllUsers() {
        const result = await this.pool.query('SELECT * FROM users');
        return result.rows;
    }
    
    async close() {
        await this.pool.end();
    }
}

module.exports = new DBService();
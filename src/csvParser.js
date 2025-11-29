const fs = require('fs');

class CSVParser {
    parseCSV(filePath) {
        const fileContent = fs.readFileSync(filePath, 'utf-8');
        const lines = fileContent.split('\n').filter(line => line.trim());
        
        if (lines.length === 0) return [];
        
        const headers = this.parseCSVLine(lines[0]);
        const records = [];
        
        for (let i = 1; i < lines.length; i++) {
            const values = this.parseCSVLine(lines[i]);
            if (values.length === headers.length) {
                records.push(this.createObject(headers, values));
            }
        }
        
        return records;
    }
    
    parseCSVLine(line) {
        const result = [];
        let current = '';
        let inQuotes = false;
        
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            
            if (char === '"') {
                inQuotes = !inQuotes;
            } else if (char === ',' && !inQuotes) {
                result.push(current.trim());
                current = '';
            } else {
                current += char;
            }
        }
        result.push(current.trim());
        
        return result;
    }
    
    createObject(headers, values) {
        const obj = {};
        
        for (let i = 0; i < headers.length; i++) {
            const path = headers[i].split('.');
            const value = values[i];
            this.setNestedProperty(obj, path, value);
        }
        
        return obj;
    }
    
    setNestedProperty(obj, path, value) {
        let current = obj;
        
        for (let i = 0; i < path.length - 1; i++) {
            const key = path[i];
            if (!current[key]) {
                current[key] = {};
            }
            current = current[key];
        }
        
        current[path[path.length - 1]] = value;
    }
}

module.exports = new CSVParser();
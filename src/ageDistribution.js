class AgeDistribution {
    calculate(users) {
        const distribution = {
            '< 20': 0,
            '20 to 40': 0,
            '40 to 60': 0,
            '> 60': 0
        };
        
        const total = users.length;
        
        users.forEach(user => {
            const age = user.age;
            
            if (age < 20) {
                distribution['< 20']++;
            } else if (age >= 20 && age < 40) {
                distribution['20 to 40']++;
            } else if (age >= 40 && age < 60) {
                distribution['40 to 60']++;
            } else {
                distribution['> 60']++;
            }
        });
        
        this.printReport(distribution, total);
    }
    
    printReport(distribution, total) {
        console.log('\n=== Age Distribution Report ===\n');
        console.log('Age-Group\t% Distribution');
        console.log('--------------------------------');
        
        for (const [ageGroup, count] of Object.entries(distribution)) {
            const percentage = total > 0 ? ((count / total) * 100).toFixed(2) : 0;
            console.log(`${ageGroup}\t\t${percentage}`);
        }
        
        console.log('\n================================\n');
    }
}

module.exports = new AgeDistribution();
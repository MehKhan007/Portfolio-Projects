import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# 1. Setup Basic Parameters
n_rows = 1000
suppliers = {
    'Local': ['Al-Qahtani Pipe', 'SADCO', 'Zamil Offshore', 'Rawabi Holding'],
    'International': ['Halliburton', 'Schlumberger', 'Baker Hughes', 'Tenaris']
}
parts = ['Gate Valve', 'Centrifugal Pump', 'Drill Bit', 'Pressure Sensor', 'Steel Pipe']
locations = ['Dammam', 'Jubail', 'Jeddah', 'Houston', 'Aberdeen', 'Singapore']

# 2. Generate Data
data = []
for i in range(n_rows):
    origin_type = random.choice(['Local', 'International'])
    supplier = random.choice(suppliers[origin_type])
    part = random.choice(parts)
    
    # IKTVA logic: Local companies have high scores (70-95%), Int'l have low (10-30%)
    iktva = random.randint(70, 95) if origin_type == 'Local' else random.randint(10, 35)
    
    # Lead Time logic: Int'l shipments take longer and have more delays
    promised = random.randint(5, 15) if origin_type == 'Local' else random.randint(30, 60)
    delay_chance = random.uniform(0, 1)
    
    # Simulate a "Red Sea Risk" factor for international shipping
    delay = 0
    if origin_type == 'International' and delay_chance > 0.6:
        delay = random.randint(10, 25) 
    elif origin_type == 'Local' and delay_chance > 0.9:
        delay = random.randint(1, 4)
        
    actual = promised + delay
    unit_cost = random.randint(500, 50000)
    
    data.append([
        f"PO-{1000+i}", datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365)),
        part, supplier, origin_type, iktva, promised, actual, unit_cost
    ])

# 3. Create DataFrame
df = pd.DataFrame(data, columns=[
    'PO_Number', 'Order_Date', 'Part_Description', 'Supplier', 
    'Origin', 'IKTVA_Score_Percent', 'Promised_Lead_Time', 'Actual_Lead_Time', 'Unit_Cost_USD'
])

# 4. Save to CSV
df.to_csv('Saudi_Oil_Supply_Chain.csv', index=False)
print("File 'Saudi_Oil_Supply_Chain.csv' has been created!")
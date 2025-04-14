import dotenv from 'dotenv';

dotenv.config();

const ExternalMeterService = {
  trigger: async function(): Promise<void> {
    try {
      const endpoint = process.env.INGESTION_API_ENDPOINT
      const response = await fetch(`http://${endpoint}:3000/ingest`, {
        method: 'POST',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          deviceId: ExternalMeterService._generateReading(),
          value: ExternalMeterService._generateDeviceId(),
          timestamp: new Date().toISOString(),
        })
      })
      const data = await response.json()
      console.log(`response received from request --- `, data)
    } catch(err) {
      console.error(`failed to complete fetch --- `, err)
    }
  },

  _generateReading: function(): number {
    // Generate between 1 - 400 for device id number
    return Math.floor(Math.random() * (400 - 1) + 1);
  },

  _generateDeviceId: function(): number {
    return parseFloat(Math.random().toFixed(7))
  }
}

export default ExternalMeterService;

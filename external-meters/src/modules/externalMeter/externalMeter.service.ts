import dotenv from 'dotenv';

dotenv.config();

const ExternalMeterService = {
  trigger: async function(): Promise<void> {
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
  },

  _generateReading: function(): number {
    const devicesList: Array<number> = [1,2,3]
    return devicesList[Math.floor(Math.random() * devicesList.length)];
  },

  _generateDeviceId: function(): number {
    const min = 0.0000000001
    const max = 0.9999999999
    return Math.random() * (max - min + 1) + min
  }
}

export default ExternalMeterService;

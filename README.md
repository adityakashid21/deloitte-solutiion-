# 🌐 IIoT Telemetry Data Reconciliation – Daikibo Industrials

This project is a high-performance Python pipeline to unify and analyze IIoT (Industrial Internet of Things) telemetry data from two different device formats used by **Daikibo Industrials**, a global heavy machinery manufacturer headquartered in Tokyo, Japan.

Deloitte is assisting Daikibo in integrating IIoT data to enable centralized, real-time monitoring and analytics.

---

## 📌 Problem Statement

Daikibo's IIoT devices output telemetry data in two formats:

- 📄 **data-1.json**
  - `timestamp`: ISO 8601 format (e.g. `"2025-06-22T08:45:00Z"`)
  - `temperature`: °C
  - `pressure`: kPa

- 📄 **data-2.json**
  - `timestamp`: milliseconds since epoch
  - `temperature`: °F
  - `pressure`: PSI

The goal is to convert both into a **unified format**:
```json
{
  "device_id": "XYZ-123",
  "timestamp": 1656512345678,
  "temperature_celsius": 65.0,
  "pressure_kpa": 101.3
}
├── data-1.json                # Input file (format A)
├── data-2.json                # Input file (format B)
├── main.py                    # Core processor logic
├── enterprise-telemetry-output.json  # Final unified result
├── telemetry_processing.log   # System logs
├── processing_errors.log      # Invalid entries (if any)
└── README.md                  # This file



▶️ How to Run
pip install numpy
python main.py


📊 Sample Output (Metadata + Analysis)

{
  "metadata": {
    "processing_timestamp": "2025-06-22T10:32:00.000Z",
    "total_files_processed": 2,
    "processing_stats": {
      "total_entries": 8000,
      "processed_entries": 7980,
      "failed_entries": 20,
      "processing_time": 2.85,
      "throughput": 2800.35,
      "memory_usage": 0.0
    },
    "analysis": {
      "unique_devices": 120,
      "temperature_stats": {
        "min": 23.4,
        "max": 97.8,
        "mean": 56.1,
        "std": 8.4
      },
      "pressure_stats": {
        "min": 95.6,
        "max": 112.7,
        "mean": 101.2,
        "std": 3.1
      }
    }
  },
  "telemetry_data": [
    {
      "device_id": "DXB-101",
      "timestamp": 1656512345678,
      "temperature_celsius": 68.3,
      "pressure_kpa": 101.3
    },
    ...
  ]
}

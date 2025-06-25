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
# deloitte-solutiion-

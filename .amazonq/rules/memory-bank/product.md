# Weather-Watcher: Product Overview

## Purpose
Distributed event-driven meteorological monitoring system that continuously ingests weather data, processes real-time business rules, and delivers WhatsApp notifications to users via Twilio.

## Key Features
- Real-time weather data ingestion via Open-Meteo API (configurable polling, default 15 min)
- 5 alert types: daily umbrella, UV protection, thermal comfort, imminent rain, heatwave
- Event-driven architecture with Apache Kafka (Publisher/Subscriber pattern)
- Stateful complex event processing (heatwave detection with sliding window)
- WhatsApp delivery via Twilio SDK

## Alert Business Rules
1. **DAILY_UMBRELLA**: Triggered at configured hour (default 6h) if daily precipitation > 70%
2. **DAILY_UV_PROTECTION**: Triggered at configured hour if UV index > 3
3. **DAILY_THERMAL**: Triggered at configured hour if max temperature > 30°C
4. **IMMINENT_RAIN**: Triggered in near-real-time if next-hour precipitation >= 70%
5. **HEATWAVE**: Triggered when N consecutive readings (default 3) exceed critical threshold (default 35°C)

## Data Flow
```
Open-Meteo API → WeatherProducerApp → [weather-raw topic]
                                              ↓
                          ┌───────────────────┼───────────────────┐
                          ↓                   ↓                   ↓
               DailySummaryConsumer  HourlyRainConsumer  HeatwaveDetector
                          └───────────────────┼───────────────────┘
                                              ↓
                                    [weather-alerts topic]
                                              ↓
                                WhatsAppNotificationConsumer
                                              ↓
                                         Twilio API → WhatsApp
```

## Target Users
Individual users who want proactive weather alerts on WhatsApp for a configured geographic location (default: Vitória-ES, Brazil).
